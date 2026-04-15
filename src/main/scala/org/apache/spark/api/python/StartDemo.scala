
package org.apache.spark.api.python

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateViewCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.flow.{GraphNodeType, SQLContractedFlow, SQLFlow}
import org.apache.spark.sql.flow.{SQLFlowGraphEdge, SQLFlowGraphNode}
import org.apache.spark.sql.flow.sink.{Neo4jAuraFieldLineageSink, Neo4jFieldLineageWriteStats}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable}
import org.apache.spark.sql.types.StructType





object StartDemo {
  private object GraphType {
    val Full = 1
    val Contracted = 2
    val DirectTableField = 3
  }

  private val graphTypeOverride: Option[Int] = Some(GraphType.DirectTableField)
//  private val graphTypeOverride: Option[Int] = Some(GraphType.Full)
//  private val graphTypeOverride: Option[Int] = Some(GraphType.Contracted)
//  private val graphTypeOverride: Option[Int] = None
  private val Neo4jUri = "neo4j://127.0.0.1:7687"
  private val Neo4jUser = "neo4j"
  private val Neo4jPassword = "wx123456.."

  private case class MaterializedTarget(node: SQLFlowGraphNode, writeColumnNames: Seq[String])

  private case class LineagePlan(
      inputPlan: LogicalPlan,
      materializedTarget: Option[MaterializedTarget])

  private case class SqlSource(
      sourceIndex: Int,
      sourceFile: String,
      sourcePath: String,
      sql: String)

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
      errorMessage: String = "")

  def main(args: Array[String]): Unit = {
    val graphType = graphTypeOverride.getOrElse(GraphType.Full)
    val sqlDir = args.headOption.map(new File(_))
      .getOrElse(new File("src/main/resources/demo"))
    val reportFile = new File("output/sqlflow-debug/batch-parse-report.tsv")
    val scriptSummaryFile = new File("output/sqlflow-debug/batch-script-summary.tsv")
    val neo4jSink = Neo4jAuraFieldLineageSink(Neo4jUri, Neo4jUser, Neo4jPassword)
    val warehouseDir = Files.createTempDirectory("spark-script-import-warehouse-")
    val sparkSession = SparkSession.builder()
      .appName("sql-script-import-service")
      .master("local[1]")
      .config("spark.ui.enabled", "true")
      .config("spark.executor.heartbeatInterval", "60s")
      .config("spark.network.timeout", "1800s")
      .config("spark.rpc.askTimeout", "1800s")
      .config("spark.sql.broadcastTimeout", "1800")
      .config("spark.sql.storeAssignmentPolicy", "LEGACY")
      .config("spark.sql.maxPlanStringLength", "1048576")
      .config("spark.sql.debug.maxToStringFields", "1048576")
      .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
      .config("hive.metastore.uris", "thrift://hive-metsatore2.58dns.org:9083")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .enableHiveSupport()
      .getOrCreate()

    try {
      val rows = readSqlSources(sqlDir)
      val records = rows.flatMap(processSqlRow(_, sparkSession, graphType, neo4jSink))
      writeReport(reportFile, records)
      writeScriptSummary(scriptSummaryFile, records)
      printSummary(sqlDir, reportFile, scriptSummaryFile, rows.size, records)
    } finally {
      sparkSession.stop()
    }
  }

  private val LocalJarDir = "/Users/zz/work/jars"
  private val AddJarPathPattern = """(?i)\b(add\s+jar\s+)(['"]?)(\S+?\.jar)\2""".r
  private val CustomSetVariablePattern =
    """(?is)\bset\s+(?:hivevar:|hiveconf:)?@?[A-Za-z_][A-Za-z0-9_]*\s*=\s*(.*)""".r

  private def readSqlSources(sqlDir: File): Seq[SqlSource] = {
    if (!sqlDir.isDirectory) {
      throw new IllegalArgumentException(s"SQL directory does not exist: ${sqlDir.getAbsolutePath}")
    }
    val sqlFiles = Option(sqlDir.listFiles()).getOrElse(Array.empty)
      .filter(file => file.isFile && file.getName.toLowerCase(Locale.ROOT).endsWith(".sql"))
      .sortBy(_.getName)
    sqlFiles.zipWithIndex.map { case (file, index) =>
      SqlSource(
        index + 1,
        file.getName,
        file.getAbsolutePath,
        new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8))
    }.toSeq
  }

  private def processSqlRow(
      row: SqlSource,
      sparkSession: SparkSession,
      graphType: Int,
      neo4jSink: Neo4jAuraFieldLineageSink): Seq[ParseRecord] = {
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
        new IllegalArgumentException("No SQL statement found")))
    } else {
      statements.zipWithIndex.map { case (statement, index) =>
        val statementIndex = index + 1
        if (isSetupStatement(statement)) {
          executeSetupStatement(row, statementIndex, statement, sparkSession)
        } else {
          parseStatement(row, statementIndex, statement, sparkSession, graphType, neo4jSink)
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
      sparkSession: SparkSession): ParseRecord = {
    if (isHiveVarStatement(statement)) {
      successfulRecord(row, statementIndex, statementType(statement), 0, 0)
    } else {
      Try(sparkSession.sql(statement).collect()) match {
        case Success(_) =>
          successfulRecord(row, statementIndex, statementType(statement), 0, 0)
        case Failure(err) =>
          failedRecord(row, statementIndex, statementType(statement), err)
      }
    }
  }

  private def isHiveVarStatement(statement: String): Boolean = {
    CustomSetVariablePattern.findFirstIn(statement).nonEmpty
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
      neo4jSink: Neo4jAuraFieldLineageSink): ParseRecord = {
    Try {
      val parsed = sparkSession.sessionState.sqlParser.parsePlan(statement)
      val analyzed = sparkSession.sessionState.analyzer.execute(parsed)
      sparkSession.sessionState.analyzer.checkAnalysis(analyzed)
      val (nodes, edges) = lineageGraph(analyzed, graphType)
      val writeStats = appendLineageGraph(neo4jSink, row, graphType, nodes, edges)
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
        failedRecord(row, statementIndex, statementType(statement), err)
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
      neo4jSink: Neo4jAuraFieldLineageSink,
      row: SqlSource,
      graphType: Int,
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge]): Neo4jFieldLineageWriteStats = {
    if (nodes.nonEmpty || edges.nonEmpty) {
      val options = neo4jOptions(row, graphType)
      val writeStats = neo4jSink.plannedWriteStats(nodes, edges, options)
      neo4jSink.append(nodes, edges, options)
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
      err: Throwable): ParseRecord = {
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
      Option(err.getMessage).getOrElse(""))
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
      writer.println(
        "status\tsource_index\tsource_file\tsource_path\tstatement_index\tstatement_type\t" +
          "node_count\tedge_count\terror_class\terror_message")
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
          record.errorMessage).map(escapeTsv).mkString("\t"))
      }
    } finally {
      writer.close()
    }
  }

  private def writeScriptSummary(reportFile: File, records: Seq[ParseRecord]): Unit = {
    Option(reportFile.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(reportFile, "UTF-8")
    try {
      writer.println(
        "status\tsource_index\tsource_file\tsource_path\tstatement_count\t" +
          "success_count\tfailure_count\twritten_node_count\twritten_edge_count")
      records.groupBy(record => (record.sourceIndex, record.sourceFile, record.sourcePath))
        .toSeq
        .sortBy { case ((sourceIndex, sourceFile, _), _) => (sourceIndex, sourceFile) }
        .foreach { case ((sourceIndex, sourceFile, sourcePath), scriptRecords) =>
          val successRecords = scriptRecords.filter(_.status == "SUCCESS")
          val failureCount = scriptRecords.count(_.status == "FAILURE")
          val status = if (failureCount == 0) "SUCCESS" else "FAILURE"
          writer.println(Seq(
            status,
            sourceIndex.toString,
            sourceFile,
            sourcePath,
            scriptRecords.size.toString,
            successRecords.size.toString,
            failureCount.toString,
            successRecords.map(_.nodeCount).sum.toString,
            successRecords.map(_.edgeCount).sum.toString).map(escapeTsv).mkString("\t"))
        }
    } finally {
      writer.close()
    }
  }

  private def escapeTsv(value: String): String = {
    value.replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')
  }

  private def printSummary(
      sqlDir: File,
      reportFile: File,
      scriptSummaryFile: File,
      sourceCount: Int,
      records: Seq[ParseRecord]): Unit = {
    val successCount = records.count(_.status == "SUCCESS")
    val failureCount = records.count(_.status == "FAILURE")
    // scalastyle:off println
    println(s"=== SQL dir: ${sqlDir.getAbsolutePath} ===")
    println(s"=== SQL files: $sourceCount ===")
    println(s"=== Statements: ${records.size}, success: $successCount, failure: $failureCount ===")
    println(s"=== Parse report: ${reportFile.getAbsolutePath} ===")
    println(s"=== Script summary: ${scriptSummaryFile.getAbsolutePath} ===")
    records.filter(_.status == "FAILURE").take(20).foreach { record =>
      println(
        s"[FAILURE] file=${record.sourceFile}, " +
          s"statement=${record.statementIndex}, type=${record.statementType}, " +
          s"${record.errorClass}: ${record.errorMessage}")
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
