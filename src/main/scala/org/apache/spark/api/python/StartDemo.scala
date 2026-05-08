
package org.apache.spark.api.python

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateViewCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.flow.{GraphNodeType, SQLContractedFlow, SQLFlow}
import org.apache.spark.sql.flow.{SQLFlowGraphEdge, SQLFlowGraphNode}
import org.apache.spark.sql.flow.sink.{
  FieldLineageSink,
  FileFieldLineageSink,
  Neo4jAuraFieldLineageSink,
  Neo4jFieldLineageWriteStats
}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.types.StructType





object StartDemo {
  private object GraphType {
    val Full = 1
    val Contracted = 2
    val DirectTableField = 3
  }

  private val graphTypeOverride: Option[Int] = Some(GraphType.DirectTableField)

  private val Neo4jUri = "neo4j://127.0.0.1:7687"
  private val Neo4jUser = "neo4j"
  private val Neo4jPassword = "wx123456.."
  private val ReportTimestampFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
  private val DefaultSinkMode = "file"
  private val DefaultOutputRootPrefix = "parallel-run-"
  private val StatementPreviewMaxLength = 240
  private val WithAddJarDirSuffix = "_with_add_jar"
  private val InsertTargetPattern =
    """(?is)\binsert\s+(?:overwrite|into)\s+table\s+([`A-Za-z0-9_.]+)""".r
  private val FromOrJoinPattern =
    """(?is)\b(?:from|join)\s+([`A-Za-z0-9_.]+)""".r
  private val ScriptSummaryHeader =
    "status\tsource_index\tsource_file\tsource_path\tstatement_count\t" +
      "success_count\tfailure_count\twritten_node_count\twritten_edge_count"
  private val ParseReportHeader =
    "status\tsource_index\tsource_file\tsource_path\tstatement_index\tstatement_type\t" +
      "node_count\tedge_count\terror_class\terror_message\troot_cause_class\t" +
      "root_cause_message\tcause_chain\tstatement_preview\ttable_diagnostics\t" +
      "viewfs_candidates"

  private case class MaterializedTarget(node: SQLFlowGraphNode, writeColumnNames: Seq[String])

  private case class LineagePlan(
      inputPlan: LogicalPlan,
      materializedTarget: Option[MaterializedTarget])

  private case class OutputLayout(
      rootDir: File,
      logsDir: File,
      reportsDir: File,
      statusDir: File,
      lineageDir: File,
      reportFile: File,
      scriptSummaryFile: File,
      statusFile: File,
      failuresFile: File)

  private case class SqlSource(
      sourceIndex: Int,
      sourceFile: String,
      sourcePath: String,
      sql: String,
      requiresWithAddJarDir: Boolean)

  private case class ParseRecord(
      sourceIndex: Int,
      sourceFile: String,
      sourcePath: String,
      statementIndex: Int,
      statementType: String,
      status: String,
      nodeCount: Int,
      edgeCount: Int,
      errorClass: String = "",
      errorMessage: String = "",
      rootCauseClass: String = "",
      rootCauseMessage: String = "",
      causeChain: String = "",
      statementPreview: String = "",
      stackTrace: String = "",
      tableDiagnostics: Seq[String] = Nil,
      viewfsCandidates: Seq[String] = Nil)

  private case class ScriptSummaryRecord(
      status: String,
      sourceIndex: Int,
      sourceFile: String,
      sourcePath: String,
      statementCount: Int,
      successCount: Int,
      failureCount: Int,
      writtenNodeCount: Int,
      writtenEdgeCount: Int,
      firstFailure: Option[ParseRecord])

  def main(args: Array[String]): Unit = {
    val graphType = graphTypeOverride.getOrElse(GraphType.Full)
    val sqlInput = args.headOption.map(new File(_))
      .getOrElse(new File("input/sqls"))
    val outputRoot = args.lift(1).map(new File(_))
      .getOrElse(defaultOutputRoot())
    val sinkMode = args.lift(2)
      .orElse(sys.env.get("SQLFLOW_SINK_MODE"))
      .getOrElse(DefaultSinkMode)
    val outputLayout = createOutputLayout(outputRoot)
    val lineageSink = createLineageSink(sinkMode, outputLayout.lineageDir)
    val warehouseRoot = Files.createTempDirectory("spark-script-import-warehouse-")

    val rows = readSqlSources(sqlInput)
    val scriptSummaries = mutable.ArrayBuffer.empty[ScriptSummaryRecord]
    val records = rows.flatMap { row =>
      val scriptTaskName = taskName(row)
      val reportFile = new File(outputLayout.reportsDir, s"$scriptTaskName.report.tsv")
      val logFile = new File(outputLayout.logsDir, s"$scriptTaskName.log")
      val perScriptStatusFile =
        new File(outputLayout.statusDir, s"$scriptTaskName.status.tsv")
      val scriptRecords = Try {
        val warehouseDir = Files.createDirectories(
          warehouseRoot.resolve(f"script-${row.sourceIndex}%05d"))
        val sparkSession = createSparkSession(warehouseDir.toAbsolutePath.toString)
        val registeredJarPaths = mutable.Set.empty[String]
        val registeredTempFunctions = mutable.Map.empty[String, String]
        try {
          processSqlRow(
            row,
            sparkSession,
            graphType,
            lineageSink,
            registeredJarPaths,
            registeredTempFunctions)
        } finally {
          sparkSession.stop()
        }
      }.recover {
        case NonFatal(err) =>
          Seq(failedRecord(row, 0, "SCRIPT", err, row.sql, None, None))
      }.get
      val scriptSummary = summarizeScriptRecords(scriptRecords)
      val archivedTo = archiveSqlSource(row, scriptSummary.status)
      scriptSummaries += scriptSummary
      writeReport(reportFile, scriptRecords)
      writeSingleScriptSummary(perScriptStatusFile, scriptSummary)
      writeScriptLog(logFile, scriptSummary, archivedTo)
      printScriptResult(scriptSummary)
      archivedTo.foreach(target => println(s"[ARCHIVE] ${row.sourcePath} -> ${target.getAbsolutePath}"))
      scriptRecords
    }

    writeReport(outputLayout.reportFile, records)
    writeScriptSummaries(outputLayout.scriptSummaryFile, scriptSummaries.toSeq)
    writeScriptSummaries(outputLayout.statusFile, scriptSummaries.toSeq)
    writeScriptSummaries(
      outputLayout.failuresFile,
      scriptSummaries.filter(_.status == "FAILURE").toSeq)
    printSummary(
      sqlInput,
      outputLayout.reportFile,
      outputLayout.scriptSummaryFile,
      rows.size,
      records,
      outputLayout)
  }

  private def defaultOutputRoot(): File = {
    val timestamp = LocalDateTime.now().format(ReportTimestampFormatter)
    new File("output/sqlflow-debug", s"$DefaultOutputRootPrefix$timestamp")
  }

  private def createOutputLayout(outputRoot: File): OutputLayout = {
    val logsDir = new File(outputRoot, "logs")
    val reportsDir = new File(outputRoot, "reports")
    val statusDir = new File(outputRoot, "status")
    val lineageDir = new File(outputRoot, "lineage")
    Seq(outputRoot, logsDir, reportsDir, statusDir, lineageDir).foreach(_.mkdirs())
    OutputLayout(
      outputRoot,
      logsDir,
      reportsDir,
      statusDir,
      lineageDir,
      new File(reportsDir, "batch-parse-report.tsv"),
      timestampedReportFile(new File(statusDir, "batch-script-summary.tsv")),
      new File(statusDir, "status.tsv"),
      new File(statusDir, "failures.tsv"))
  }

  private def taskName(row: SqlSource): String = {
    f"${row.sourceIndex}%05d_${sanitizeFileName(row.sourceFile)}"
  }

  private[python] def sanitizeFileName(value: String): String = {
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.replaceAll("""[\\/:*?"<>|\p{Cntrl}]+""", "_"))
      .map(_.replaceAll("\\s+", "_"))
      .map(_.replaceAll("_+", "_"))
      .map(_.stripPrefix("_").stripSuffix("_"))
      .filter(_.nonEmpty)
      .getOrElse("unknown")
  }

  private[python] def archiveDirName(status: String, requiresWithAddJarDir: Boolean): String = {
    val prefix = if (status == "SUCCESS") "success" else "failed"
    if (requiresWithAddJarDir) prefix + WithAddJarDirSuffix else prefix
  }

  private[python] def resolveArchiveTarget(
      sourceFile: File,
      requiresWithAddJarDir: Boolean,
      status: String): File = {
    val parentDir = Option(sourceFile.getParentFile).getOrElse(new File("."))
    val rootDir = Option(parentDir.getParentFile).getOrElse(parentDir)
    new File(new File(rootDir, archiveDirName(status, requiresWithAddJarDir)), sourceFile.getName)
  }

  private def archiveSqlSource(row: SqlSource, status: String): Option[File] = {
    val sourceFile = new File(row.sourcePath)
    if (!sourceFile.isFile) {
      None
    } else {
      Some(moveToArchive(sourceFile, row.requiresWithAddJarDir, status))
    }
  }

  private[python] def moveToArchive(
      sourceFile: File,
      requiresWithAddJarDir: Boolean,
      status: String): File = {
    val targetFile = resolveArchiveTarget(sourceFile, requiresWithAddJarDir, status)
    Option(targetFile.getParentFile).foreach(_.mkdirs())
    Files.move(
      sourceFile.toPath,
      targetFile.toPath,
      StandardCopyOption.REPLACE_EXISTING)
    targetFile
  }

  private def writeScriptLog(
      logFile: File,
      summary: ScriptSummaryRecord,
      archivedTo: Option[File]): Unit = {
    val lines = mutable.ArrayBuffer.empty[String]
    lines +=
      s"[SCRIPT] status=${summary.status}, file=${summary.sourceFile}, " +
        s"statements=${summary.statementCount}, success=${summary.successCount}, " +
        s"failure=${summary.failureCount}, writtenNodes=${summary.writtenNodeCount}, " +
        s"writtenEdges=${summary.writtenEdgeCount}"
    summary.firstFailure.foreach { failure =>
      lines +=
        s"[SCRIPT-FAILURE] file=${failure.sourceFile}, " +
          s"statement=${failure.statementIndex}, type=${failure.statementType}, " +
          s"${failure.errorClass}: ${failure.errorMessage}"
      if (failure.statementPreview.nonEmpty) {
        lines += s"[SCRIPT-FAILURE-STMT] ${failure.statementPreview}"
      }
      if (failure.rootCauseClass.nonEmpty) {
        lines += s"[SCRIPT-FAILURE-ROOT] ${failure.rootCauseClass}: ${failure.rootCauseMessage}"
      }
      if (failure.causeChain.nonEmpty) {
        lines += s"[SCRIPT-FAILURE-CAUSE-CHAIN] ${failure.causeChain}"
      }
      failure.tableDiagnostics.foreach { line =>
        lines += s"[SCRIPT-FAILURE-TABLE] $line"
      }
      failure.viewfsCandidates.foreach { line =>
        lines += s"[SCRIPT-FAILURE-VIEWFS-CANDIDATE] $line"
      }
      if (failure.stackTrace.nonEmpty) {
        lines += "[SCRIPT-FAILURE-STACKTRACE-BEGIN]"
        lines += failure.stackTrace
        lines += "[SCRIPT-FAILURE-STACKTRACE-END]"
      }
    }
    archivedTo.foreach(target => lines += s"[ARCHIVE] ${target.getAbsolutePath}")
    writeLines(logFile, lines.toSeq)
  }

  private def writeLines(file: File, lines: Seq[String]): Unit = {
    Option(file.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(file, "UTF-8")
    try {
      lines.foreach(writer.println)
    } finally {
      writer.close()
    }
  }

  private def writeSingleScriptSummary(reportFile: File, summary: ScriptSummaryRecord): Unit = {
    writeScriptSummaries(reportFile, Seq(summary))
  }

  private def writeScriptSummaries(
      reportFile: File,
      summaries: Seq[ScriptSummaryRecord]): Unit = {
    Option(reportFile.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(reportFile, "UTF-8")
    try {
      writer.println(ScriptSummaryHeader)
      summaries.foreach { summary =>
        writer.println(scriptSummaryLine(summary))
      }
    } finally {
      writer.close()
    }
  }

  private def createLineageSink(sinkMode: String, outputDir: File): FieldLineageSink = {
    sinkMode.trim.toLowerCase(Locale.ROOT) match {
      case "file" =>
        FileFieldLineageSink(outputDir)
      case "neo4j" =>
        Neo4jAuraFieldLineageSink(Neo4jUri, Neo4jUser, Neo4jPassword)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported lineage sink mode '$other'. Expected 'file' or 'neo4j'.")
    }
  }

  private def createSparkSession(warehouseDir: String): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("sql-script-import-service")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.maxPlanStringLength", "1048576")
      .config("spark.sql.debug.maxToStringFields", "1048576")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.default.parallelism", "1")
      .config("spark.sql.extensions", "org.apache.spark.sql.flow.EmptyCatalogScanExtensions")
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config(
        "spark.sql.catalog.dataplat_hadoop_catalog",
        "org.apache.spark.sql.flow.DataplatHadoopCatalog")
      .config("hive.metastore.uris", "thrift://hive-metsatore2.58dns.org:9083")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("spark.sql.legacy.parser.havingWithoutGroupByAsWhere", "true")
      .enableHiveSupport()
      .getOrCreate()
    loadHadoopResources(sparkSession)
    sparkSession.sparkContext.setLogLevel("error")
    sparkSession

  }

  private def loadHadoopResources(sparkSession: SparkSession): Unit = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    val resources = Seq("hadoop/core-site.xml", "hadoop/mountTable.xml")
    resources.foreach { resource =>
      resolveHadoopResource(resource) match {
        case Some(path) =>
          hadoopConf.addResource(path)
          println(s"[HADOOP-CONF] loaded resource=$resource from=${path.toString}")
        case None =>
          println(s"[HADOOP-CONF] missing resource=$resource")
      }
    }
    normalizeViewFsImplementations(hadoopConf)

    val homeMount = Option(hadoopConf.get("fs.viewfs.mounttable.58-cluster.link./home"))
      .getOrElse("")
    if (homeMount.nonEmpty) {
      println(s"[HADOOP-CONF] fs.viewfs.mounttable.58-cluster.link./home=$homeMount")
    } else {
      println("[HADOOP-CONF] fs.viewfs.mounttable.58-cluster.link./home is not configured")
    }

    val nameservices = Option(hadoopConf.get("dfs.nameservices")).getOrElse("")
    if (nameservices.nonEmpty) {
      println(s"[HADOOP-CONF] dfs.nameservices=$nameservices")
    } else {
      println("[HADOOP-CONF] dfs.nameservices is not configured")
    }

    val failoverProvider = Option(
      hadoopConf.get("dfs.client.failover.proxy.provider.58-cluster")).getOrElse("")
    if (failoverProvider.nonEmpty) {
      println(s"[HADOOP-CONF] dfs.client.failover.proxy.provider.58-cluster=$failoverProvider")
    } else {
      println("[HADOOP-CONF] dfs.client.failover.proxy.provider.58-cluster is not configured")
    }
  }

  private def normalizeViewFsImplementations(
      hadoopConf: org.apache.hadoop.conf.Configuration): Unit = {
    val replacements = Seq(
      "fs.viewfs.impl" ->
        ("org.apache.hadoop.fs.viewfs.ViewFileSystem",
          "org.apache.hadoop.hdfs.ViewFsRedirectDistributedFileSystem"),
      "fs.AbstractFileSystem.viewfs.impl" ->
        ("org.apache.hadoop.fs.viewfs.ViewFs",
          "org.apache.hadoop.hdfs.ViewFsRedirectHdfs"))

    replacements.foreach { case (key, (fallbackClass, incompatibleClass)) =>
      val configured = Option(hadoopConf.get(key)).map(_.trim).filter(_.nonEmpty)
      configured.foreach { className =>
        if (!isClassAvailable(className)) {
          hadoopConf.set(key, fallbackClass)
          println(
            s"[HADOOP-CONF] override $key from=$className to=$fallbackClass " +
              s"(missing class, expected compatible replacement for $incompatibleClass)")
        } else {
          println(s"[HADOOP-CONF] using $key=$className")
        }
      }
      if (configured.isEmpty) {
        hadoopConf.set(key, fallbackClass)
        println(s"[HADOOP-CONF] default $key=$fallbackClass")
      }
    }
  }

  private def isClassAvailable(className: String): Boolean = {
    Try(Class.forName(className, false, getClass.getClassLoader)).isSuccess
  }

  private def resolveHadoopResource(resource: String): Option[Path] = {
    val classpathResource = Option(getClass.getClassLoader.getResource(resource))
      .map(url => new Path(url.toString))
    classpathResource.orElse {
      val sourceFile = new File(s"src/main/resources/$resource")
      if (sourceFile.isFile) Some(new Path(sourceFile.getAbsolutePath)) else None
    }
  }

  private val LocalJarDir = "/Users/zz/work/jars"
  private val AddJarPathPattern = """(?i)\b(add\s+jar\s+)(['"]?)(\S+?\.jar)\2""".r
  private val JarPathTokenPattern = """(?i)(?:['"]([^'"]+?\.jar)['"]|(\S+?\.jar))""".r
  private val CustomSetVariablePattern =
    """(?is)\bset\s+(?:hivevar:|hiveconf:)?@?[A-Za-z_][A-Za-z0-9_]*\s*=\s*(.*)""".r
  private val CreateTempFunctionPrefixPattern =
    """(?is)^\s*create\s+temp(?:orary)?\s+function\s+(?:if\s+not\s+exists\s+)?""".r

  private def readSqlSources(sqlInput: File): Seq[SqlSource] = {
    if (sqlInput.isFile) {
      if (!sqlInput.getName.toLowerCase(Locale.ROOT).endsWith(".sql")) {
        throw new IllegalArgumentException(s"SQL file must end with .sql: ${sqlInput.getAbsolutePath}")
      }
      val sqlText = new String(Files.readAllBytes(sqlInput.toPath), StandardCharsets.UTF_8)
      Seq(SqlSource(
        1,
        sqlInput.getName,
        sqlInput.getAbsolutePath,
        sqlText,
        requiresWithAddJarDirectory(sqlInput, sqlText)))
    } else if (sqlInput.isDirectory) {
      val sqlFiles = Option(sqlInput.listFiles()).getOrElse(Array.empty)
        .filter(file => file.isFile && file.getName.toLowerCase(Locale.ROOT).endsWith(".sql"))
        .sortBy(_.getName)
      sqlFiles.zipWithIndex.map { case (file, index) =>
        val sqlText = new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8)
        SqlSource(
          index + 1,
          file.getName,
          file.getAbsolutePath,
          sqlText,
          requiresWithAddJarDirectory(file, sqlText))
      }.toSeq
    } else {
      throw new IllegalArgumentException(s"SQL input does not exist: ${sqlInput.getAbsolutePath}")
    }
  }

  private[python] def requiresWithAddJarDirectory(sourceFile: File, sqlText: String): Boolean = {
    val parentName = Option(sourceFile.getParentFile).map(_.getName).getOrElse("")
    parentName.endsWith(WithAddJarDirSuffix) || {
      val statements = splitStatements(stripSqlComments(sqlText))
      statements.exists(isWithAddJarStatement)
    }
  }

  private def isWithAddJarStatement(statement: String): Boolean = {
    val upper = statement.trim.toUpperCase(Locale.ROOT)
    upper.startsWith("ADD JAR ") ||
      upper.startsWith("CREATE TEMPORARY FUNCTION ") ||
      upper.startsWith("CREATE TEMP FUNCTION ") ||
      upper.startsWith("CREATE OR REPLACE TEMPORARY FUNCTION ") ||
      upper.startsWith("CREATE OR REPLACE TEMP FUNCTION ")
  }

  private def processSqlRow(
      row: SqlSource,
      sparkSession: SparkSession,
      graphType: Int,
      lineageSink: FieldLineageSink,
      registeredJarPaths: mutable.Set[String],
      registeredTempFunctions: mutable.Map[String, String]): Seq[ParseRecord] = {
    // scalastyle:off println
    println(s"=== Processing SQL file: ${row.sourceFile} ===")
    // scalastyle:on println
    val sqlText = prepareSqlText(row.sql)
    val statements = splitStatements(sqlText).filterNot(isFilteredStatement)
    if (statements.isEmpty) {
      Seq(failedRecord(
        row,
        0,
        "EMPTY",
        new IllegalArgumentException("No SQL statement found"),
        "",
        None,
        None))
    } else {
      statements.zipWithIndex.map { case (statement, index) =>
        val statementIndex = index + 1
        if (isSetupStatement(statement)) {
          executeSetupStatement(
            row,
            statementIndex,
            statement,
            sparkSession,
            registeredJarPaths,
            registeredTempFunctions)
        } else {
          parseStatement(row, statementIndex, statement, sparkSession, graphType, lineageSink)
        }
      }
    }
  }

  private[python] def prepareSqlText(sqlText: String): String = {
    localizeAddJarPaths(stripSqlComments(sqlText))
  }

  private[python] def stripSqlComments(sqlText: String): String = {
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

  private[python] def localizeAddJarPaths(sqlText: String): String = {
    AddJarPathPattern.replaceAllIn(sqlText, matched => {
      val path = matched.group(3)
      scala.util.matching.Regex.quoteReplacement(
        s"${matched.group(1)}${matched.group(2)}${localJarPath(path)}" +
          matched.group(2))
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

  private def executeSetupStatement(
      row: SqlSource,
      statementIndex: Int,
      statement: String,
      sparkSession: SparkSession,
      registeredJarPaths: mutable.Set[String],
      registeredTempFunctions: mutable.Map[String, String]): ParseRecord = {
    if (isHiveVarStatement(statement)) {
      successfulRecord(row, statementIndex, statementType(statement), 0, 0)
    } else if (isDuplicateAddJarRegistration(statement, registeredJarPaths)) {
      successfulRecord(row, statementIndex, statementType(statement), 0, 0)
    } else if (isDuplicateTempFunctionRegistration(
        statement,
        sparkSession,
        registeredTempFunctions)) {
      successfulRecord(row, statementIndex, statementType(statement), 0, 0)
    } else {
      Try(sparkSession.sql(statement).collect()) match {
        case Success(_) =>
          rememberAddJarRegistration(statement, registeredJarPaths)
          rememberTempFunctionRegistration(statement, registeredTempFunctions)
          successfulRecord(row, statementIndex, statementType(statement), 0, 0)
        case Failure(err) =>
          failedRecord(
            row,
            statementIndex,
            statementType(statement),
            err,
            statement,
            None,
            None)
      }
    }
  }

  private def isHiveVarStatement(statement: String): Boolean = {
    CustomSetVariablePattern.findFirstIn(statement).nonEmpty
  }

  private[python] def extractAddJarPaths(statement: String): Seq[String] = {
    val upper = statement.trim.toUpperCase(Locale.ROOT)
    if (!upper.startsWith("ADD JAR ")) {
      Nil
    } else {
      JarPathTokenPattern.findAllMatchIn(statement).map { matched =>
        Option(matched.group(1)).getOrElse(matched.group(2))
      }.toSeq
    }
  }

  private[python] def extractAddJarFileNames(statement: String): Seq[String] = {
    extractAddJarPaths(statement).map(path => new File(path).getName.toLowerCase(Locale.ROOT))
  }

  private[python] def isDuplicateAddJarRegistration(
      statement: String,
      registeredJarPaths: scala.collection.Set[String]): Boolean = {
    val jarPaths = extractAddJarPaths(statement)
    jarPaths.nonEmpty && jarPaths.forall(registeredJarPaths.contains)
  }

  private[python] def rememberAddJarRegistration(
      statement: String,
      registeredJarPaths: mutable.Set[String]): Unit = {
    extractAddJarPaths(statement).foreach(registeredJarPaths.add)
  }

  private[python] def extractTempFunctionName(statement: String): Option[String] = {
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

  private def normalizeRegistrationStatement(statement: String): String = {
    statement.trim.replaceAll("\\s+", " ")
  }

  private[python] def isDuplicateTempFunctionRegistration(
      statement: String,
      registeredTempFunctions: scala.collection.Map[String, String]): Boolean = {
    val normalizedStatement = normalizeRegistrationStatement(statement)
    extractTempFunctionName(statement).exists { functionName =>
      registeredTempFunctions.get(functionName).contains(normalizedStatement)
    }
  }

  private[python] def isDuplicateTempFunctionRegistration(
      statement: String,
      sparkSession: SparkSession,
      registeredTempFunctions: scala.collection.Map[String, String]): Boolean = {
    isDuplicateTempFunctionRegistration(statement, registeredTempFunctions)
  }

  private[python] def rememberTempFunctionRegistration(
      statement: String,
      registeredTempFunctions: mutable.Map[String, String]): Unit = {
    val normalizedStatement = normalizeRegistrationStatement(statement)
    extractTempFunctionName(statement).foreach { functionName =>
      registeredTempFunctions.getOrElseUpdate(functionName, normalizedStatement)
    }
  }

  private[python] def isFilteredStatement(statement: String): Boolean = {
    val normalized = statement.trim.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ")
    normalized.startsWith("DROP TABLE ") ||
      normalized.startsWith("TRUNCATE TABLE ") ||
      normalized.startsWith("DELETE FROM ") ||
      (normalized.startsWith("ALTER TABLE ") &&
        normalized.contains(" DROP ") &&
        normalized.contains(" PARTITION"))
  }

  private def parseStatement(
      row: SqlSource,
      statementIndex: Int,
      statement: String,
      sparkSession: SparkSession,
      graphType: Int,
      lineageSink: FieldLineageSink): ParseRecord = {
    val parsedPlan = Try(sparkSession.sessionState.sqlParser.parsePlan(statement))
    parsedPlan match {
      case Failure(err) =>
        failedRecord(
          row,
          statementIndex,
          statementType(statement),
          err,
          statement,
          Some(sparkSession),
          None)
      case Success(parsed) =>
//        val analyzed = sparkSession.sessionState.analyzer.execute(parsed)
        Try {
      val analyzed = sparkSession.sessionState.analyzer.execute(parsed)
      sparkSession.sessionState.analyzer.checkAnalysis(analyzed)
      val (nodes, edges) = lineageGraph(analyzed, graphType)
      val writeStats = appendLineageGraph(lineageSink, row, graphType, nodes, edges)
      successfulRecord(
        row,
        statementIndex,
        statementType(statement),
        writeStats.nodeCount,
        writeStats.edgeCount)
        } match {
          case Success(record) =>
            record
          case Failure(err) =>
            failedRecord(
              row,
              statementIndex,
              statementType(statement),
              err,
              statement,
              Some(sparkSession),
              Some(parsed))
        }
    }
  }

  private def lineageGraph(
      plan: LogicalPlan,
      graphType: Int): (Seq[SQLFlowGraphNode], Seq[SQLFlowGraphEdge]) = {
    val lineagePlan = resolveLineagePlan(plan)
    if (lineagePlan.inputPlan.output.isEmpty && lineagePlan.materializedTarget.isEmpty) {
      (Nil, Nil)
    } else {
      val sqlFlow = graphType match {
        case GraphType.Contracted => SQLContractedFlow()
        case _ => SQLFlow()
      }
      val (baseNodes, baseEdges) = sqlFlow.planToSQLFlow(lineagePlan.inputPlan)
      val (nodes, edges) = attachMaterializedTarget(
        baseNodes,
        baseEdges,
        lineagePlan.materializedTarget)
      (nodes, edges)
    }
  }

  private def appendLineageGraph(
      lineageSink: FieldLineageSink,
      row: SqlSource,
      graphType: Int,
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge]): Neo4jFieldLineageWriteStats = {
    if (nodes.nonEmpty || edges.nonEmpty) {
      val options = neo4jOptions(row, graphType)
      val writeStats = lineageSink.plannedWriteStats(nodes, edges, options)
      lineageSink.append(nodes, edges, options)
      writeStats
    } else {
      Neo4jFieldLineageWriteStats(0, 0)
    }
  }

  private def neo4jOptions(row: SqlSource, graphType: Int): Map[String, String] = {
    Map(
      "graphMode" -> (graphType match {
        case GraphType.DirectTableField => "direct_table_field"
        case _ => "full"
      }),
      "sqlFileName" -> row.sourceFile,
      "sqlFilePath" -> row.sourcePath)
  }

  private def successfulRecord(
      row: SqlSource,
      statementIndex: Int,
      statementType: String,
      nodeCount: Int,
      edgeCount: Int): ParseRecord = {
    ParseRecord(
      row.sourceIndex,
      row.sourceFile,
      row.sourcePath,
      statementIndex,
      statementType,
      "SUCCESS",
      nodeCount,
      edgeCount)
  }

  private def failedRecord(
      row: SqlSource,
      statementIndex: Int,
      statementType: String,
      err: Throwable,
      statement: String,
      sparkSession: Option[SparkSession],
      parsedPlan: Option[LogicalPlan]): ParseRecord = {
    val causeChain = throwableChain(err)
    val rootCause = causeChain.lastOption.getOrElse(err)
    val failureDiagnostics =
      for {
        session <- sparkSession
      } yield {
        parsedPlan.map(plan => describeFailureTables(session, plan, statement))
          .filter(_._1.nonEmpty)
          .getOrElse(describeFailureTablesFromStatement(session, statement))
      }
    val tableDiagnostics = failureDiagnostics.map(_._1).getOrElse(Nil)
    val viewfsCandidates = failureDiagnostics.map(_._2).getOrElse(Nil)
    ParseRecord(
      row.sourceIndex,
      row.sourceFile,
      row.sourcePath,
      statementIndex,
      statementType,
      "FAILURE",
      0,
      0,
      err.getClass.getName,
      Option(err.getMessage).getOrElse(""),
      rootCause.getClass.getName,
      Option(rootCause.getMessage).getOrElse(""),
      formatCauseChain(causeChain),
      statementPreview(statement),
      renderStackTrace(err),
      tableDiagnostics,
      viewfsCandidates)
  }

  private def describeFailureTables(
      sparkSession: SparkSession,
      parsedPlan: LogicalPlan,
      statement: String): (Seq[String], Seq[String]) = {
    val targetIdentifiers = collectTargetIdentifiers(parsedPlan)
    val targetKeySet = targetIdentifiers.map(_.unquotedString.toLowerCase(Locale.ROOT)).toSet
    val sourceIdentifiers = collectReferencedIdentifiers(parsedPlan)
      .filterNot(identifier => targetKeySet.contains(identifier.unquotedString.toLowerCase(Locale.ROOT)))
    val diagnostics = distinctDiagnostics(
      targetIdentifiers.map(identifier => describeCatalogTable(sparkSession, identifier, "target")) ++
        sourceIdentifiers.map(identifier => describeCatalogTable(sparkSession, identifier, "source")))
    if (diagnostics.nonEmpty) {
      val viewfsLines = diagnostics.filter(_.toLowerCase(Locale.ROOT).contains("viewfs://"))
      (diagnostics, viewfsLines)
    } else {
      describeFailureTablesFromStatement(sparkSession, statement)
    }
  }

  private def describeFailureTablesFromStatement(
      sparkSession: SparkSession,
      statement: String): (Seq[String], Seq[String]) = {
    val targetIdentifiers = InsertTargetPattern.findAllMatchIn(statement)
      .flatMap(matched => textToTableIdentifier(matched.group(1)))
      .toSeq
    val targetKeySet = targetIdentifiers.map(_.unquotedString.toLowerCase(Locale.ROOT)).toSet
    val sourceIdentifiers = FromOrJoinPattern.findAllMatchIn(statement)
      .flatMap(matched => textToTableIdentifier(matched.group(1)))
      .filterNot(identifier => targetKeySet.contains(identifier.unquotedString.toLowerCase(Locale.ROOT)))
      .toSeq
    val diagnostics = distinctDiagnostics(
      targetIdentifiers.map(identifier => describeCatalogTable(sparkSession, identifier, "target")) ++
        sourceIdentifiers.map(identifier => describeCatalogTable(sparkSession, identifier, "source")))
    val viewfsLines = diagnostics.filter(_.toLowerCase(Locale.ROOT).contains("viewfs://"))
    (diagnostics, viewfsLines)
  }

  private def collectTargetIdentifiers(plan: LogicalPlan): Seq[TableIdentifier] = {
    distinctIdentifiers(plan.collect {
      case insert: InsertIntoStatement =>
        resolveIdentifier(insert.table)
    }.flatten)
  }

  private def collectReferencedIdentifiers(plan: LogicalPlan): Seq[TableIdentifier] = {
    distinctIdentifiers(plan.collect {
      case relation: UnresolvedRelation =>
        multipartIdentifierToTableIdentifier(relation.multipartIdentifier)
    }.flatten)
  }

  private def multipartIdentifierToTableIdentifier(parts: Seq[String]): Option[TableIdentifier] = {
    parts.lastOption.map { table =>
      val database = if (parts.size > 1) Some(parts.dropRight(1).mkString(".")) else None
      TableIdentifier(table, database)
    }
  }

  private def distinctIdentifiers(identifiers: Seq[TableIdentifier]): Seq[TableIdentifier] = {
    identifiers
      .groupBy(_.unquotedString.toLowerCase(Locale.ROOT))
      .values
      .map(_.head)
      .toSeq
      .sortBy(_.unquotedString.toLowerCase(Locale.ROOT))
  }

  private def textToTableIdentifier(raw: String): Option[TableIdentifier] = {
    val cleaned = raw.trim.stripSuffix(",").replace("`", "")
    if (cleaned.isEmpty) {
      None
    } else {
      multipartIdentifierToTableIdentifier(cleaned.split('.').toSeq)
    }
  }

  private def distinctDiagnostics(lines: Seq[String]): Seq[String] = {
    lines.distinct.sortBy(_.toLowerCase(Locale.ROOT))
  }

  private def describeCatalogTable(
      sparkSession: SparkSession,
      identifier: TableIdentifier,
      role: String): String = {
    val tableName = identifier.unquotedString
    val metadataAttempt = Try(sparkSession.sessionState.catalog.getTableMetadata(identifier))
    metadataAttempt.map { table =>
      val tableType = table.tableType.name
      val provider = table.provider.getOrElse("")
      val location = table.storage.locationUri.map(_.toString).getOrElse("")
      Seq(
        s"role=$role",
        s"table=$tableName",
        s"tableType=$tableType",
        if (provider.nonEmpty) s"provider=$provider" else "",
        if (location.nonEmpty) s"location=$location" else "location=<empty>"
      ).filter(_.nonEmpty).mkString(", ")
    }.recover {
      case catalogErr =>
        s"role=$role, table=$tableName, metadataError=${catalogErr.getClass.getName}: " +
          Option(catalogErr.getMessage).getOrElse("")
    }.get
  }

  private def throwableChain(err: Throwable): Seq[Throwable] = {
    val chain = mutable.ArrayBuffer.empty[Throwable]
    var current = err
    while (current != null && !chain.exists(_ eq current)) {
      chain += current
      current = current.getCause
    }
    chain.toSeq
  }

  private def formatCauseChain(chain: Seq[Throwable]): String = {
    chain.map { throwable =>
      s"${throwable.getClass.getName}: ${Option(throwable.getMessage).getOrElse("")}".trim
    }.mkString(" <- ")
  }

  private def statementPreview(statement: String): String = {
    val normalized = statement.trim.replaceAll("\\s+", " ")
    if (normalized.length <= StatementPreviewMaxLength) {
      normalized
    } else {
      normalized.take(StatementPreviewMaxLength - 3) + "..."
    }
  }

  private def renderStackTrace(err: Throwable): String = {
    val builder = new StringBuilder
    appendThrowable(builder, err, prefix = "")
    builder.toString.trim
  }

  private def appendThrowable(
      builder: StringBuilder,
      throwable: Throwable,
      prefix: String): Unit = {
    if (throwable == null) {
      return
    }
    builder.append(prefix)
    builder.append(throwable.getClass.getName)
    val message = Option(throwable.getMessage).getOrElse("")
    if (message.nonEmpty) {
      builder.append(": ")
      builder.append(message)
    }
    builder.append('\n')
    throwable.getStackTrace.foreach { element =>
      builder.append('\t')
      builder.append("at ")
      builder.append(element.toString)
      builder.append('\n')
    }
    throwable.getSuppressed.foreach { suppressed =>
      appendThrowable(builder, suppressed, prefix + "Suppressed: ")
    }
    if (throwable.getCause != null) {
      appendThrowable(builder, throwable.getCause, prefix + "Caused by: ")
    }
  }

  private def statementType(statement: String): String = {
    val upper = statement.trim.toUpperCase(Locale.ROOT)
    if (upper.startsWith("CREATE TABLE")) {
      "CREATE TABLE"
    } else if (upper.startsWith("CREATE VIEW")) {
      "CREATE VIEW"
    } else if (upper.startsWith("INSERT")) {
      "INSERT"
    } else if (upper.startsWith("WITH")) {
      "WITH"
    } else if (upper.startsWith("SELECT")) {
      "SELECT"
    } else if (upper.startsWith("ADD JAR")) {
      "ADD JAR"
    } else if (upper.startsWith("SET")) {
      "SET"
    } else {
      upper.takeWhile(!_.isWhitespace)
    }
  }

  private def writeReport(reportFile: File, records: Seq[ParseRecord]): Unit = {
    Option(reportFile.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(reportFile, "UTF-8")
    try {
      writer.println(ParseReportHeader)
      records.foreach { record =>
        writer.println(Seq(
          record.status,
          record.sourceIndex.toString,
          record.sourceFile,
          record.sourcePath,
          record.statementIndex.toString,
          record.statementType,
          record.nodeCount.toString,
          record.edgeCount.toString,
          record.errorClass,
          record.errorMessage,
          record.rootCauseClass,
          record.rootCauseMessage,
          record.causeChain,
          record.statementPreview,
          record.tableDiagnostics.mkString(" || "),
          record.viewfsCandidates.mkString(" || ")).map(escapeTsv).mkString("\t"))
      }
    } finally {
      writer.close()
    }
  }

  private def initializeScriptSummaryFile(reportFile: File): Unit = {
    Option(reportFile.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(reportFile, "UTF-8")
    try {
      writer.println(ScriptSummaryHeader)
    } finally {
      writer.close()
    }
  }

  private def appendScriptSummary(reportFile: File, summary: ScriptSummaryRecord): Unit = {
    Option(reportFile.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(new OutputStreamWriter(
      new FileOutputStream(reportFile, true),
      StandardCharsets.UTF_8))
    try {
      writer.println(scriptSummaryLine(summary))
    } finally {
      writer.close()
    }
  }

  private def scriptSummaryLine(summary: ScriptSummaryRecord): String = {
    Seq(
      summary.status,
      summary.sourceIndex.toString,
      summary.sourceFile,
      summary.sourcePath,
      summary.statementCount.toString,
      summary.successCount.toString,
      summary.failureCount.toString,
      summary.writtenNodeCount.toString,
      summary.writtenEdgeCount.toString).map(escapeTsv).mkString("\t")
  }

  private def escapeTsv(value: String): String = {
    value.replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')
  }

  private def summarizeScriptRecords(scriptRecords: Seq[ParseRecord]): ScriptSummaryRecord = {
    val firstRecord = scriptRecords.head
    val successRecords = scriptRecords.filter(_.status == "SUCCESS")
    val failureRecords = scriptRecords.filter(_.status == "FAILURE")
    ScriptSummaryRecord(
      if (failureRecords.isEmpty) "SUCCESS" else "FAILURE",
      firstRecord.sourceIndex,
      firstRecord.sourceFile,
      firstRecord.sourcePath,
      scriptRecords.size,
      successRecords.size,
      failureRecords.size,
      successRecords.map(_.nodeCount).sum,
      successRecords.map(_.edgeCount).sum,
      failureRecords.headOption)
  }

  private def printScriptResult(summary: ScriptSummaryRecord): Unit = {
    // scalastyle:off println
    println(
      s"[SCRIPT] status=${summary.status}, file=${summary.sourceFile}, " +
        s"statements=${summary.statementCount}, success=${summary.successCount}, " +
        s"failure=${summary.failureCount}, writtenNodes=${summary.writtenNodeCount}, " +
        s"writtenEdges=${summary.writtenEdgeCount}")
    summary.firstFailure.foreach { failure =>
      println(
        s"[SCRIPT-FAILURE] file=${failure.sourceFile}, " +
          s"statement=${failure.statementIndex}, type=${failure.statementType}, " +
        s"${failure.errorClass}: ${failure.errorMessage}")
      if (failure.statementPreview.nonEmpty) {
        println(s"[SCRIPT-FAILURE-STMT] ${failure.statementPreview}")
      }
      if (failure.rootCauseClass.nonEmpty) {
        println(
          s"[SCRIPT-FAILURE-ROOT] ${failure.rootCauseClass}: ${failure.rootCauseMessage}")
      }
      if (failure.causeChain.nonEmpty) {
        println(s"[SCRIPT-FAILURE-CAUSE-CHAIN] ${failure.causeChain}")
      }
      failure.tableDiagnostics.foreach { line =>
        println(s"[SCRIPT-FAILURE-TABLE] $line")
      }
      failure.viewfsCandidates.foreach { line =>
        println(s"[SCRIPT-FAILURE-VIEWFS-CANDIDATE] $line")
      }
      if (failure.stackTrace.nonEmpty) {
        println("[SCRIPT-FAILURE-STACKTRACE-BEGIN]")
        println(failure.stackTrace)
        println("[SCRIPT-FAILURE-STACKTRACE-END]")
      }
    }
    // scalastyle:on println
  }

  private def timestampedReportFile(baseFile: File): File = {
    val timestamp = LocalDateTime.now().format(ReportTimestampFormatter)
    new File(baseFile.getParentFile, withTimestampSuffix(baseFile.getName, timestamp))
  }

  private[python] def withTimestampSuffix(fileName: String, timestamp: String): String = {
    val extensionIndex = fileName.lastIndexOf('.')
    if (extensionIndex >= 0) {
      s"${fileName.substring(0, extensionIndex)}-$timestamp${fileName.substring(extensionIndex)}"
    } else {
      s"$fileName-$timestamp"
    }
  }

  private def printSummary(
      sqlInput: File,
      reportFile: File,
      scriptSummaryFile: File,
      sourceCount: Int,
      records: Seq[ParseRecord],
      outputLayout: OutputLayout): Unit = {
    val successCount = records.count(_.status == "SUCCESS")
    val failureCount = records.count(_.status == "FAILURE")
    // scalastyle:off println
    println(s"=== SQL input: ${sqlInput.getAbsolutePath} ===")
    println(s"=== Output root: ${outputLayout.rootDir.getAbsolutePath} ===")
    println(s"=== SQL files: $sourceCount ===")
    println(s"=== Statements: ${records.size}, success: $successCount, failure: $failureCount ===")
    println(s"=== Parse report: ${reportFile.getAbsolutePath} ===")
    println(s"=== Script summary: ${scriptSummaryFile.getAbsolutePath} ===")
    println(s"=== Logs dir: ${outputLayout.logsDir.getAbsolutePath} ===")
    println(s"=== Status dir: ${outputLayout.statusDir.getAbsolutePath} ===")
    println(s"=== Failures: ${outputLayout.failuresFile.getAbsolutePath} ===")
    records.filter(_.status == "FAILURE").take(20).foreach { record =>
      println(
        s"[FAILURE] file=${record.sourceFile}, " +
          s"statement=${record.statementIndex}, type=${record.statementType}, " +
          s"${record.errorClass}: ${record.errorMessage}")
      if (record.statementPreview.nonEmpty) {
        println(s"[FAILURE-STMT] ${record.statementPreview}")
      }
      if (record.rootCauseClass.nonEmpty) {
        println(s"[FAILURE-ROOT] ${record.rootCauseClass}: ${record.rootCauseMessage}")
      }
      record.viewfsCandidates.foreach { line =>
        println(s"[FAILURE-VIEWFS-CANDIDATE] $line")
      }
    }
    // scalastyle:on println
  }

  private[python] def splitStatements(sqlText: String): Seq[String] = {
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
    val upper = statement.trim.toUpperCase
    upper.startsWith("SET ") ||
      upper.startsWith("ADD JAR ") ||
      upper.startsWith("ADD FILE ") ||
      upper.startsWith("ADD ARCHIVE ") ||
      upper.startsWith("CREATE TEMPORARY FUNCTION ") ||
      upper.startsWith("CREATE TEMP FUNCTION ")
  }

  private def resolveLineagePlan(plan: LogicalPlan): LineagePlan = plan match {
    case insert: InsertIntoHiveTable =>
      LineagePlan(
        insert.query,
        Some(buildMaterializedTarget(insert.table, GraphNodeType.TableNode, insert.outputColumnNames)))

    case insert: InsertIntoHadoopFsRelationCommand =>
      LineagePlan(
        insert.query,
        insert.catalogTable.map { table =>
          buildMaterializedTarget(table, GraphNodeType.TableNode, insert.outputColumnNames)
        })

    case insert: InsertIntoDataSourceCommand =>
      LineagePlan(
        insert.query,
        insert.logicalRelation.catalogTable.map { table =>
          buildMaterializedTarget(table, GraphNodeType.TableNode, insert.query.output.map(_.name))
        })

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
      val viewColumns = if (createView.userSpecifiedColumns.nonEmpty) {
        createView.userSpecifiedColumns.map(_._1)
      } else {
        createView.plan.output.map(_.name)
      }
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
          val writeColumns = if (insert.userSpecifiedCols.nonEmpty) {
            insert.userSpecifiedCols
          } else {
            insert.query.output.map(_.name)
          }
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
    buildMaterializedTarget(
      table.identifier,
      fieldNames,
      schema,
      nodeType,
      queryColumnNames)
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
}
