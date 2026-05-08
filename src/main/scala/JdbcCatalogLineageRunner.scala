import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateViewCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.flow.{GraphNodeType, SQLFlow, SQLFlowGraphEdge, SQLFlowGraphNode}
import org.apache.spark.sql.flow.sink.{FieldLineageSink, FileFieldLineageSink, Neo4jAuraFieldLineageSink, Neo4jFieldLineageWriteStats}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutableURLClassLoader

object JdbcCatalogLineageRunner {
  private val DefaultSqlInput = new File("src/main/resources/demo")
  private val DefaultOutputDir = new File("output/sqlflow-jdbc-bootstrap")
  private val DefaultSinkMode = "file"
  private val LocalJarDir = "/Users/zz/work/jars"
  private val LocalHiveDirName = "local-hive"
  private val WarehouseDirName = "spark-warehouse"
  private val MetastoreDirName = "metastore_db"

  private val Neo4jUri = "neo4j://127.0.0.1:7687"
  private val Neo4jUser = "neo4j"
  private val Neo4jPassword = "wx123456.."

  private val AddJarPathPattern = """(?i)\b(add\s+jar\s+)(['"]?)(\S+?\.jar)\2""".r
  private val JarPathTokenPattern = """(?i)(?:['"]([^'"]+?\.jar)['"]|(\S+?\.jar))""".r
  private val CreateTempFunctionPrefixPattern =
    """(?is)^\s*create\s+temp(?:orary)?\s+function\s+(?:if\s+not\s+exists\s+)?""".r
  private val CreateTempFunctionClassPattern =
    """(?is)^\s*create\s+temp(?:orary)?\s+function\s+(?:if\s+not\s+exists\s+)?([`A-Za-z0-9_.]+)\s+as\s+['"]([^'"]+)['"]""".r

  private case class SqlSource(sourceFile: String, sourcePath: String, sql: String)
  private case class MaterializedTarget(node: SQLFlowGraphNode, writeColumnNames: Seq[String])
  private case class LineagePlan(inputPlan: LogicalPlan, materializedTarget: Option[MaterializedTarget])
  private case class TempFunctionRegistration(functionName: String, className: String)

  def main(args: Array[String]): Unit = {
    val sqlInput = args.headOption.map(new File(_)).getOrElse(DefaultSqlInput)
    val outputDir = args.lift(1).map(new File(_)).getOrElse(DefaultOutputDir)
    val sinkMode = args.lift(2).getOrElse(DefaultSinkMode)
    val lineageSink = createLineageSink(sinkMode, outputDir)
    val sqlSources = readSqlSources(sqlInput)

    sqlSources.foreach { source =>
      println(s"=== JDBC bootstrap processing: ${source.sourceFile} ===")
      val sparkSession = createSparkSession(outputDir, source)
      val bootstrapper = newBootstrapper(sparkSession)
      val driverJarClassLoader =
        new MutableURLClassLoader(Array.empty[URL], Thread.currentThread().getContextClassLoader)
      val registeredJars = mutable.Set.empty[String]
      val registeredTempFunctions = mutable.Map.empty[String, String]
      val originalContextClassLoader = Thread.currentThread().getContextClassLoader
      Thread.currentThread().setContextClassLoader(driverJarClassLoader)
      try {
        processSqlSource(
          source,
          sparkSession,
          bootstrapper,
          lineageSink,
          driverJarClassLoader,
          registeredJars,
          registeredTempFunctions)
      } finally {
        Thread.currentThread().setContextClassLoader(originalContextClassLoader)
        bootstrapper.close()
        sparkSession.stop()
      }
    }
  }

  private def createSparkSession(outputDir: File, source: SqlSource): SparkSession = {
    val localHiveRoot = Files.createDirectories(outputDir.toPath.resolve(LocalHiveDirName))
    val sessionRoot = Files.createTempDirectory(localHiveRoot, sanitizeFileName(source.sourceFile) + "-")
    val warehouseDir = Files.createDirectories(sessionRoot.resolve(WarehouseDirName)).toAbsolutePath.toString
    val metastoreDir = sessionRoot.resolve(MetastoreDirName).toAbsolutePath.toString
    val metastoreUrl = s"jdbc:derby:;databaseName=$metastoreDir;create=true"

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val sparkSession = SparkSession.builder()
      .appName("jdbc-catalog-lineage-runner")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.maxPlanStringLength", "1048576")
      .config("spark.sql.debug.maxToStringFields", "1048576")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.default.parallelism", "1")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config(
        "spark.sql.catalog.dataplat_hadoop_catalog",
        "org.apache.spark.sql.flow.DataplatHadoopCatalog")
      .config("hive.metastore.warehouse.dir", warehouseDir)
      .config("hive.metastore.uris", "")
      .config("spark.sql.hive.metastore.jars", "builtin")
      .config("javax.jdo.option.ConnectionURL", metastoreUrl)
      .config("datanucleus.schema.autoCreateAll", "true")
      .config("hive.metastore.schema.verification", "false")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    println(s"[SPARK-SESSION] sessionRoot=${sessionRoot.toAbsolutePath}")
    println(s"[SPARK-SESSION] warehouseDir=$warehouseDir")
    println(s"[SPARK-SESSION] metastoreUrl=$metastoreUrl")
    sparkSession
  }

  private def sanitizeFileName(fileName: String): String = {
    val sanitized = fileName.replaceAll("[^A-Za-z0-9._-]", "_")
    if (sanitized.nonEmpty) sanitized else "sql-source"
  }

  private def createLineageSink(sinkMode: String, outputDir: File): FieldLineageSink = {
    sinkMode.trim.toLowerCase(Locale.ROOT) match {
      case "file" =>
        FileFieldLineageSink(new File(outputDir, "lineage"))
      case "neo4j" =>
        Neo4jAuraFieldLineageSink(Neo4jUri, Neo4jUser, Neo4jPassword)
      case other =>
        throw new IllegalArgumentException(s"Unsupported sink mode: $other")
    }
  }

  private def readSqlSources(sqlInput: File): Seq[SqlSource] = {
    if (sqlInput.isFile) {
      Seq(SqlSource(
        sqlInput.getName,
        sqlInput.getAbsolutePath,
        new String(Files.readAllBytes(sqlInput.toPath), StandardCharsets.UTF_8)))
    } else if (sqlInput.isDirectory) {
      Option(sqlInput.listFiles()).getOrElse(Array.empty)
        .filter(file => file.isFile && file.getName.toLowerCase(Locale.ROOT).endsWith(".sql"))
        .sortBy(_.getName)
        .map(file => SqlSource(
          file.getName,
          file.getAbsolutePath,
          new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8)))
        .toSeq
    } else {
      throw new IllegalArgumentException(s"SQL input does not exist: ${sqlInput.getAbsolutePath}")
    }
  }

  private def processSqlSource(
      source: SqlSource,
      sparkSession: SparkSession,
      bootstrapper: BootstrapperFacade,
      lineageSink: FieldLineageSink,
      driverJarClassLoader: MutableURLClassLoader,
      registeredJars: mutable.Set[String],
      registeredTempFunctions: mutable.Map[String, String]): Unit = {
    val sqlText = prepareSqlText(source.sql)
    val statements = splitStatements(sqlText)
    statements.zipWithIndex.foreach { case (statement, index) =>
      val statementIndex = index + 1
      if (isSetupStatement(statement)) {
        executeSetupStatement(
          statement,
          sparkSession,
          driverJarClassLoader,
          registeredJars,
          registeredTempFunctions)
        println(s"[SETUP] file=${source.sourceFile}, statement=$statementIndex, type=${statementType(statement)}")
      } else {
        println(s"[BOOTSTRAP] file=${source.sourceFile}, statement=$statementIndex")
        val bootstrapResult = bootstrapper.bootstrap(statement)
        if (bootstrapResult.bootstrappedTableNames.isEmpty) {
          println(s"[BOOTSTRAP] no tables extracted for statement=$statementIndex")
        } else {
          bootstrapResult.bootstrappedTableNames.foreach { tableName =>
            println(s"[BOOTSTRAP-TABLE] $tableName")
          }
        }

        val parsed = sparkSession.sessionState.sqlParser.parsePlan(statement)
        val analyzed = sparkSession.sessionState.analyzer.execute(parsed)
        sparkSession.sessionState.analyzer.checkAnalysis(analyzed)
        val (nodes, edges) = lineageGraph(analyzed)
        appendLineageGraph(lineageSink, source, nodes, edges)
        println(
          s"[SUCCESS] file=${source.sourceFile}, statement=$statementIndex, " +
            s"type=${statementType(statement)}, nodes=${nodes.size}, edges=${edges.size}")
      }
    }
  }

  private def newBootstrapper(sparkSession: SparkSession): BootstrapperFacade = {
    val configClass = Class.forName("HiveJdbcMetadataClient$Config")
    val config = configClass.getMethod("fromLegacyHiveJdbc").invoke(null)
    val clientClass = Class.forName("HiveJdbcMetadataClient")
    val client = clientClass.getConstructor(configClass).newInstance(config).asInstanceOf[AnyRef]
    val bootstrapperClass = Class.forName("JdbcSparkCatalogBootstrapper")
    val delegate = bootstrapperClass
      .getConstructor(clientClass, classOf[SparkSession])
      .newInstance(client, sparkSession)
      .asInstanceOf[AutoCloseable]
    new BootstrapperFacade(delegate)
  }

  private def appendLineageGraph(
      lineageSink: FieldLineageSink,
      source: SqlSource,
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge]): Neo4jFieldLineageWriteStats = {
    if (nodes.nonEmpty || edges.nonEmpty) {
      val options = Map(
        "graphMode" -> "direct_table_field",
        "sqlFileName" -> source.sourceFile,
        "sqlFilePath" -> source.sourcePath)
      val writeStats = lineageSink.plannedWriteStats(nodes, edges, options)
      lineageSink.append(nodes, edges, options)
      writeStats
    } else {
      Neo4jFieldLineageWriteStats(0, 0)
    }
  }

  private def lineageGraph(plan: LogicalPlan): (Seq[SQLFlowGraphNode], Seq[SQLFlowGraphEdge]) = {
    val lineagePlan = resolveLineagePlan(plan)
    if (lineagePlan.inputPlan.output.isEmpty && lineagePlan.materializedTarget.isEmpty) {
      (Nil, Nil)
    } else {
      val sqlFlow = SQLFlow()
      val (baseNodes, baseEdges) = sqlFlow.planToSQLFlow(lineagePlan.inputPlan)
      attachMaterializedTarget(baseNodes, baseEdges, lineagePlan.materializedTarget)
    }
  }

  private def prepareSqlText(sqlText: String): String = {
    localizeAddJarPaths(stripSqlComments(sqlText))
  }

  private def stripSqlComments(sqlText: String): String = {
    val result = new StringBuilder
    var inSingleQuote = false
    var inDoubleQuote = false
    var inBacktick = false
    var inLineComment = false
    var inBlockComment = false
    var index = 0
    while (index < sqlText.length) {
      val ch = sqlText.charAt(index)
      val next = if (index + 1 < sqlText.length) sqlText.charAt(index + 1) else 0.toChar
      if (inLineComment) {
        if (ch == '\n' || ch == '\r') {
          inLineComment = false
          result.append(ch)
        }
      } else if (inBlockComment) {
        if (ch == '*' && next == '/') {
          inBlockComment = false
          index += 1
        }
      } else if (ch == '\'' && !inDoubleQuote && !inBacktick) {
        result.append(ch)
        if (inSingleQuote && next == '\'') {
          index += 1
          result.append(sqlText.charAt(index))
        } else {
          inSingleQuote = !inSingleQuote
        }
      } else if (ch == '"' && !inSingleQuote && !inBacktick) {
        inDoubleQuote = !inDoubleQuote
        result.append(ch)
      } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
        inBacktick = !inBacktick
        result.append(ch)
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '-' && next == '-') {
        inLineComment = true
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '/' && next == '*') {
        inBlockComment = true
        result.append(' ')
        index += 1
      } else {
        result.append(ch)
      }
      index += 1
    }
    result.toString()
  }

  private def localizeAddJarPaths(sqlText: String): String = {
    AddJarPathPattern.replaceAllIn(sqlText, matched => {
      val path = matched.group(3)
      scala.util.matching.Regex.quoteReplacement(
        s"${matched.group(1)}${matched.group(2)}${localJarPath(path)}${matched.group(2)}")
    })
  }

  private def localJarPath(path: String): String = {
    val normalizedPath = stripUriSchemeAndAuthority(path)
      .takeWhile(ch => ch != '?' && ch != '#')
      .replace('\\', '/')
      .stripPrefix("/")
    s"$LocalJarDir/$normalizedPath"
  }

  private def stripUriSchemeAndAuthority(path: String): String = {
    val schemeIndex = path.indexOf("://")
    if (schemeIndex >= 0) {
      val pathStart = path.indexOf('/', schemeIndex + 3)
      if (pathStart >= 0) path.substring(pathStart) else path.substring(schemeIndex + 3)
    } else {
      path.stripPrefix("hdfs:")
    }
  }

  private def splitStatements(sqlText: String): Seq[String] = {
    val statements = mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var inSingleQuote = false
    var inDoubleQuote = false
    var inBacktick = false
    var index = 0
    while (index < sqlText.length) {
      val ch = sqlText.charAt(index)
      if (ch == '\'' && !inDoubleQuote && !inBacktick) {
        current.append(ch)
        if (inSingleQuote && index + 1 < sqlText.length && sqlText.charAt(index + 1) == '\'') {
          index += 1
          current.append(sqlText.charAt(index))
        } else {
          inSingleQuote = !inSingleQuote
        }
      } else if (ch == '"' && !inSingleQuote && !inBacktick) {
        inDoubleQuote = !inDoubleQuote
        current.append(ch)
      } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
        inBacktick = !inBacktick
        current.append(ch)
      } else if (ch == ';' && !inSingleQuote && !inDoubleQuote && !inBacktick) {
        appendStatement(statements, current)
      } else {
        current.append(ch)
      }
      index += 1
    }
    appendStatement(statements, current)
    statements.toSeq
  }

  private def appendStatement(
      statements: mutable.ArrayBuffer[String],
      current: StringBuilder): Unit = {
    val statement = current.toString().trim
    if (statement.nonEmpty) {
      statements += statement
    }
    current.clear()
  }

  private def isSetupStatement(statement: String): Boolean = {
    val upper = statement.trim.toUpperCase(Locale.ROOT)
    upper.startsWith("SET ") ||
      upper.startsWith("ADD JAR ") ||
      upper.startsWith("ADD FILE ") ||
      upper.startsWith("ADD ARCHIVE ") ||
      upper.startsWith("CREATE TEMPORARY FUNCTION ") ||
      upper.startsWith("CREATE TEMP FUNCTION ")
  }

  private def executeSetupStatement(
      statement: String,
      sparkSession: SparkSession,
      driverJarClassLoader: MutableURLClassLoader,
      registeredJars: mutable.Set[String],
      registeredTempFunctions: mutable.Map[String, String]): Unit = {
    val upper = statement.trim.toUpperCase(Locale.ROOT)
    if (upper.startsWith("SET ")) {
      ()
    } else if (upper.startsWith("ADD FILE ") || upper.startsWith("ADD ARCHIVE ")) {
      ()
    } else if (isDuplicateAddJarRegistration(statement, registeredJars)) {
      ()
    } else if (isDuplicateTempFunctionRegistration(statement, registeredTempFunctions)) {
      ()
    } else {
      addJarsToDriverClassLoader(statement, driverJarClassLoader)
      if (isCreateTempFunctionStatement(statement) &&
        registerHiveTempFunction(statement, sparkSession, driverJarClassLoader)) {
        ()
      } else {
        sparkSession.sql(statement).collect()
      }
      rememberAddJarRegistration(statement, registeredJars)
      rememberTempFunctionRegistration(statement, registeredTempFunctions)
    }
  }

  private def isCreateTempFunctionStatement(statement: String): Boolean = {
    val upper = statement.trim.toUpperCase(Locale.ROOT)
    upper.startsWith("CREATE TEMPORARY FUNCTION ") || upper.startsWith("CREATE TEMP FUNCTION ")
  }

  private def addJarsToDriverClassLoader(
      statement: String,
      driverJarClassLoader: MutableURLClassLoader): Unit = {
    extractAddJarPaths(statement)
      .map(localJarPath)
      .map(new File(_))
      .filter(file => file.isFile)
      .foreach { file =>
        driverJarClassLoader.addURL(file.toURI.toURL)
      }
  }

  private def extractAddJarPaths(statement: String): Seq[String] = {
    val upper = statement.trim.toUpperCase(Locale.ROOT)
    if (!upper.startsWith("ADD JAR ")) {
      Nil
    } else {
      JarPathTokenPattern.findAllMatchIn(statement).map { matched =>
        Option(matched.group(1)).getOrElse(matched.group(2))
      }.toSeq
    }
  }

  private def isDuplicateAddJarRegistration(
      statement: String,
      registeredJarPaths: scala.collection.Set[String]): Boolean = {
    val jarPaths = extractAddJarPaths(statement)
    jarPaths.nonEmpty && jarPaths.forall(registeredJarPaths.contains)
  }

  private def rememberAddJarRegistration(
      statement: String,
      registeredJarPaths: mutable.Set[String]): Unit = {
    extractAddJarPaths(statement).foreach(registeredJarPaths.add)
  }

  private def extractTempFunctionName(statement: String): Option[String] = {
    CreateTempFunctionPrefixPattern.findPrefixMatchOf(statement).flatMap { prefix =>
      val remainder = statement.substring(prefix.end).trim
      val identifier = new StringBuilder
      var inBacktick = false
      var index = 0
      while (index < remainder.length && (inBacktick || !remainder.charAt(index).isWhitespace)) {
        val ch = remainder.charAt(index)
        if (ch == '`') {
          inBacktick = !inBacktick
        }
        identifier.append(ch)
        index += 1
      }
      val normalized = identifier.toString().trim.replace("`", "").toLowerCase(Locale.ROOT)
      Option(normalized).filter(_.nonEmpty)
    }
  }

  private def extractTempFunctionRegistration(statement: String): Option[TempFunctionRegistration] = {
    CreateTempFunctionClassPattern.findFirstMatchIn(statement).map { matched =>
      TempFunctionRegistration(
        matched.group(1).replace("`", "").trim,
        matched.group(2).trim)
    }
  }

  private def registerHiveTempFunction(
      statement: String,
      sparkSession: SparkSession,
      driverJarClassLoader: ClassLoader): Boolean = {
    extractTempFunctionRegistration(statement).exists { registration =>
      val functionClass = loadFunctionClass(registration.className, driverJarClassLoader)
      functionClass.exists { clazz =>
        val functionBuilder = buildHiveFunctionBuilder(registration.functionName, clazz)
        functionBuilder.exists { builder =>
          val functionRegistry = sparkSession.sessionState.functionRegistry
          val expressionInfo = new ExpressionInfo(clazz.getCanonicalName, registration.functionName)
          functionRegistry.registerFunction(
            new FunctionIdentifier(registration.functionName),
            expressionInfo,
            builder)
          true
        }
      }
    }
  }

  private def loadFunctionClass(
      className: String,
      classLoader: ClassLoader): Option[Class[_]] = {
    try {
      Some(Class.forName(className, true, classLoader))
    } catch {
      case NonFatal(_) => None
    }
  }

  private def buildHiveFunctionBuilder(
      functionName: String,
      functionClass: Class[_]): Option[Seq[Expression] => Expression] = {
    if (classOf[org.apache.hadoop.hive.ql.exec.UDF].isAssignableFrom(functionClass)) {
      Some(children => newHiveFunctionExpression(
        "org.apache.spark.sql.hive.HiveSimpleUDF",
        functionName,
        functionClass,
        children))
    } else if (classOf[org.apache.hadoop.hive.ql.udf.generic.GenericUDF].isAssignableFrom(functionClass)) {
      Some(children => newHiveFunctionExpression(
        "org.apache.spark.sql.hive.HiveGenericUDF",
        functionName,
        functionClass,
        children))
    } else if (classOf[org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver]
      .isAssignableFrom(functionClass)) {
      Some(children => newHiveFunctionExpression(
        "org.apache.spark.sql.hive.HiveUDAFFunction",
        functionName,
        functionClass,
        children,
        java.lang.Boolean.FALSE,
        Int.box(hiveUdafDefaultInt("$lessinit$greater$default$5")),
        Int.box(hiveUdafDefaultInt("$lessinit$greater$default$6"))))
    } else if (classOf[org.apache.hadoop.hive.ql.exec.UDAF].isAssignableFrom(functionClass)) {
      Some(children => newHiveFunctionExpression(
        "org.apache.spark.sql.hive.HiveUDAFFunction",
        functionName,
        functionClass,
        children,
        java.lang.Boolean.TRUE,
        Int.box(hiveUdafDefaultInt("$lessinit$greater$default$5")),
        Int.box(hiveUdafDefaultInt("$lessinit$greater$default$6"))))
    } else if (classOf[org.apache.hadoop.hive.ql.udf.generic.GenericUDTF].isAssignableFrom(functionClass)) {
      Some(children => newHiveFunctionExpression(
        "org.apache.spark.sql.hive.HiveGenericUDTF",
        functionName,
        functionClass,
        children))
    } else {
      None
    }
  }

  private def newHiveFunctionExpression(
      expressionClassName: String,
      functionName: String,
      functionClass: Class[_],
      children: Seq[Expression],
      extraArgs: AnyRef*): Expression = {
    val originalContextClassLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(functionClass.getClassLoader)
    try {
      val wrapper = newHiveFunctionWrapper(functionClass)
      val expressionClass = Class.forName(expressionClassName)
      val constructor = expressionClass.getConstructors.find(_.getParameterCount == 3 + extraArgs.size)
        .getOrElse(throw new IllegalStateException(
          s"Failed to find constructor for $expressionClassName with ${3 + extraArgs.size} parameters"))
      val args = Seq(functionName, wrapper, children.asInstanceOf[AnyRef]) ++ extraArgs
      constructor.newInstance(args.map(_.asInstanceOf[AnyRef]): _*).asInstanceOf[Expression]
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextClassLoader)
    }
  }

  private def newHiveFunctionWrapper(functionClass: Class[_]): AnyRef = {
    val wrapperClass = Class.forName("org.apache.spark.sql.hive.HiveShim$HiveFunctionWrapper")
    val constructor = wrapperClass.getConstructor(classOf[String], classOf[Object], classOf[Class[_]])
    constructor.newInstance(functionClass.getName, null, null).asInstanceOf[AnyRef]
  }

  private def hiveUdafDefaultInt(methodName: String): Int = {
    val moduleClass = Class.forName("org.apache.spark.sql.hive.HiveUDAFFunction$")
    val module = moduleClass.getField("MODULE$").get(null)
    moduleClass.getMethod(methodName).invoke(module).asInstanceOf[Int]
  }

  private def normalizeRegistrationStatement(statement: String): String = {
    statement.trim.replaceAll("\\s+", " ")
  }

  private def isDuplicateTempFunctionRegistration(
      statement: String,
      registeredTempFunctions: scala.collection.Map[String, String]): Boolean = {
    val normalizedStatement = normalizeRegistrationStatement(statement)
    extractTempFunctionName(statement).exists { functionName =>
      registeredTempFunctions.get(functionName).contains(normalizedStatement)
    }
  }

  private def rememberTempFunctionRegistration(
      statement: String,
      registeredTempFunctions: mutable.Map[String, String]): Unit = {
    val normalizedStatement = normalizeRegistrationStatement(statement)
    extractTempFunctionName(statement).foreach { functionName =>
      registeredTempFunctions.getOrElseUpdate(functionName, normalizedStatement)
    }
  }

  private def statementType(statement: String): String = {
    statement.trim.takeWhile(!_.isWhitespace).toUpperCase(Locale.ROOT) match {
      case "" => "UNKNOWN"
      case other => other
    }
  }

  private def resolveLineagePlan(plan: LogicalPlan): LineagePlan = plan match {
    case insert: InsertIntoHiveTable =>
      LineagePlan(
        insert.query,
        Some(buildMaterializedTarget(insert.table, GraphNodeType.TableNode, insert.outputColumnNames)))

    case insert: InsertIntoHadoopFsRelationCommand =>
      LineagePlan(
        insert.query,
        insert.catalogTable.map(table =>
          buildMaterializedTarget(table, GraphNodeType.TableNode, insert.outputColumnNames)))

    case insert: InsertIntoDataSourceCommand =>
      LineagePlan(
        insert.query,
        insert.logicalRelation.catalogTable.map(table =>
          buildMaterializedTarget(table, GraphNodeType.TableNode, insert.query.output.map(_.name))))

    case ctas: CreateHiveTableAsSelectCommand =>
      LineagePlan(
        ctas.query,
        Some(buildCtasMaterializedTarget(
          ctas.tableDesc,
          ctas.query,
          GraphNodeType.TableNode,
          ctas.outputColumnNames)))

    case ctas: CreateDataSourceTableAsSelectCommand =>
      LineagePlan(
        ctas.query,
        Some(buildCtasMaterializedTarget(
          ctas.table,
          ctas.query,
          GraphNodeType.TableNode,
          ctas.outputColumnNames)))

    case createView: CreateViewCommand =>
      val viewColumns =
        if (createView.userSpecifiedColumns.nonEmpty) createView.userSpecifiedColumns.map(_._1)
        else createView.plan.output.map(_.name)
      LineagePlan(
        createView.plan,
        Some(buildMaterializedTarget(
          createView.name,
          viewColumns,
          createView.plan.schema,
          GraphNodeType.ViewNode,
          viewColumns)))

    case insert: InsertIntoStatement =>
      LineagePlan(
        insert.query,
        resolveIdentifier(insert.table).map { ident =>
          val writeColumns =
            if (insert.userSpecifiedCols.nonEmpty) insert.userSpecifiedCols
            else insert.query.output.map(_.name)
          buildMaterializedTarget(
            ident,
            insert.query.output.map(_.name),
            insert.query.schema,
            GraphNodeType.TableNode,
            writeColumns)
        })

    case other =>
      LineagePlan(other, None)
  }

  private def buildMaterializedTarget(
      table: CatalogTable,
      nodeType: GraphNodeType.Value,
      writeColumnNames: Seq[String]): MaterializedTarget = {
    buildMaterializedTarget(
      table.identifier,
      table.schema.fieldNames.toSeq,
      table.schema,
      nodeType,
      writeColumnNames)
  }

  private def buildCtasMaterializedTarget(
      table: CatalogTable,
      query: LogicalPlan,
      nodeType: GraphNodeType.Value,
      outputColumnNames: Seq[String]): MaterializedTarget = {
    val queryColumnNames =
      if (outputColumnNames.nonEmpty) outputColumnNames else query.output.map(_.name)
    val fieldNames =
      if (table.schema.fields.nonEmpty) table.schema.fieldNames.toSeq else queryColumnNames
    val schema =
      if (table.schema.fields.nonEmpty) table.schema else renameSchema(fieldNames, query.schema)
    buildMaterializedTarget(table.identifier, fieldNames, schema, nodeType, queryColumnNames)
  }

  private def buildMaterializedTarget(
      identifier: TableIdentifier,
      fieldNames: Seq[String],
      schema: StructType,
      nodeType: GraphNodeType.Value,
      writeColumnNames: Seq[String]): MaterializedTarget = {
    val qualifiedName = identifier.unquotedString
    val targetNode = SQLFlowGraphNode(
      qualifiedName,
      qualifiedName,
      fieldNames,
      renameSchema(fieldNames, schema).toDDL,
      nodeType,
      isCached = false)
    MaterializedTarget(targetNode, writeColumnNames.toSeq)
  }

  private def renameSchema(fieldNames: Seq[String], schema: StructType): StructType = {
    if (fieldNames.size == schema.fields.length) {
      StructType(fieldNames.zip(schema.fields).map { case (name, field) =>
        field.copy(name = name)
      }.toArray)
    } else {
      schema
    }
  }

  private def resolveIdentifier(plan: LogicalPlan): Option[TableIdentifier] = plan match {
    case relation: UnresolvedRelation =>
      val parts = relation.multipartIdentifier
      parts.lastOption.map { table =>
        val database = if (parts.size > 1) Some(parts.dropRight(1).mkString(".")) else None
        TableIdentifier(table, database)
      }
    case _ =>
      None
  }

  private def attachMaterializedTarget(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      materializedTarget: Option[MaterializedTarget]): (Seq[SQLFlowGraphNode], Seq[SQLFlowGraphEdge]) = {
    materializedTarget match {
      case Some(target) =>
        nodes.find(_.tpe == GraphNodeType.QueryNode).map { queryNode =>
          val targetEdges = buildTargetEdges(queryNode, target)
          val mergedNodes = upsertNode(nodes, target.node)
          (mergedNodes, edges ++ targetEdges)
        }.getOrElse((upsertNode(nodes, target.node), edges))
      case None =>
        (nodes, edges)
    }
  }

  private def buildTargetEdges(
      queryNode: SQLFlowGraphNode,
      target: MaterializedTarget): Seq[SQLFlowGraphEdge] = {
    val targetIndexByName = target.node.attributeNames.zipWithIndex.map { case (name, index) =>
      name.toLowerCase(Locale.ROOT) -> index
    }.toMap
    val effectiveWriteColumns =
      if (target.writeColumnNames.nonEmpty) target.writeColumnNames else queryNode.attributeNames
    effectiveWriteColumns.zipWithIndex.flatMap { case (columnName, fromIdx) =>
      targetIndexByName.get(columnName.toLowerCase(Locale.ROOT)).orElse {
        if (fromIdx < target.node.attributeNames.size) Some(fromIdx) else None
      }.map { toIdx =>
        SQLFlowGraphEdge(queryNode.uniqueId, Some(fromIdx), target.node.uniqueId, Some(toIdx))
      }
    }
  }

  private def upsertNode(
      nodes: Seq[SQLFlowGraphNode],
      targetNode: SQLFlowGraphNode): Seq[SQLFlowGraphNode] = {
    nodes.find(_.uniqueId == targetNode.uniqueId).map { existingNode =>
      val mergedNode = existingNode.copy(
        ident = targetNode.ident,
        attributeNames = targetNode.attributeNames,
        schemaDDL = targetNode.schemaDDL,
        tpe = targetNode.tpe,
        isCached = existingNode.isCached || targetNode.isCached)
      mergedNode.props ++= existingNode.props
      nodes.filterNot(_.uniqueId == targetNode.uniqueId) :+ mergedNode
    }.getOrElse(nodes :+ targetNode)
  }

  private final class BootstrapperFacade(private val delegate: AutoCloseable) extends AutoCloseable {
    private val bootstrapMethod = delegate.getClass.getMethod("bootstrap", classOf[String])

    def bootstrap(statement: String): BootstrapResultFacade = {
      val result = bootstrapMethod.invoke(delegate, statement)
      new BootstrapResultFacade(result)
    }

    override def close(): Unit = delegate.close()
  }

  private final class BootstrapResultFacade(private val delegate: AnyRef) {
    private val getBootstrappedTablesMethod = delegate.getClass.getMethod("getBootstrappedTables")

    def bootstrappedTableNames: Seq[String] = {
      getBootstrappedTablesMethod.invoke(delegate)
        .asInstanceOf[java.util.List[AnyRef]]
        .asScala
        .map { table =>
          table.getClass.getMethod("getQualifiedTableName").invoke(table).toString
        }
        .toSeq
    }
  }
}
