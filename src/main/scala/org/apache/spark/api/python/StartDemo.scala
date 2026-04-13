
package org.apache.spark.api.python

import java.io.File
import java.nio.file.Files
import java.time.LocalDate
import java.util.Locale

import scala.io.Source
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, CreateViewCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.flow.{GraphNodeType, SQLContractedFlow, SQLFlow, SQLFlowGraphEdge, SQLFlowGraphNode}
import org.apache.spark.sql.flow.sink.{GraphVizMetadataSink, GraphVizSink, Neo4jAuraFieldLineageSink}
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

  private case class MaterializedTarget(node: SQLFlowGraphNode, writeColumnNames: Seq[String])

  private case class LineagePlan(
      inputPlan: LogicalPlan,
      materializedTarget: Option[MaterializedTarget])

  def main(args: Array[String]): Unit = {
    val graphType = graphTypeOverride.getOrElse(GraphType.Full)
    val contracted = graphType == GraphType.Contracted
    val neo4jUri = "neo4j://127.0.0.1:7687"
    val neo4jUser = "neo4j"
    val neo4jPasswd = "wx123456.."
    val warehouseDir = Files.createTempDirectory("spark-script-import-warehouse-")
    val sparkSession = SparkSession.builder()
      .appName("sql-script-import-service")
      .master("local[1]")
      .config("spark.ui.enabled", "true")
      .config("spark.executor.heartbeatInterval", "60s")
      .config("spark.network.timeout", "1800s")
      .config("spark.rpc.askTimeout", "1800s")
      .config("spark.sql.broadcastTimeout", "1800")
      .config("spark.sql.maxPlanStringLength", "1048576")
      .config("spark.sql.debug.maxToStringFields", "1048576")
      .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
      .config("hive.metastore.uris", "thrift://hive-metsatore2.58dns.org:9083")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .enableHiveSupport()
      .getOrCreate()

    val sqlFile = new File(
//      "src/main/resources/sql/ads_bi_offline_store_operating_data_center_v3_full_1d-门店经营看板总部口径v3.sql"
//      "src/main/resources/sql/回收对账单-dw_trade_t_account_statement_recycle_detail_full_1d.sql"
//      "src/main/resources/sql/城市经营看板-二奢-tmp_recycle_lux_order_data_v3.sql"
//"src/main/resources/sql/城市经营看板-配件-tmp_recycle_fit_order_data_v2.sql"
//"src/main/resources/sql/城市经营看板-黄金-tmp_recycle_gold_order_data_v3.sql"
//"src/main/resources/sql/门店-回收零售收入数据-dm_offline_store_income_data_full_1d.sql"
//      "src/main/resources/sql/门店-月度分润数据-dm_offline_store_share_data_full_1d.sql"
//"src/main/resources/sql/门店-月度活跃基本信息-dm_offline_store_action_data_full_1d.sql"
//"src/main/resources/sql/门店-线下门店员工成本-dm_offline_store_emp_cost_full_1d.sql"

//      "src/main/resources/sql/dw_recycle_order_amt_data_full_1d.sql"
//"src/main/resources/sql/hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_recycle_price_full_1d.sql"
      //      "src/main/resources/sql/回收对账单-dw_trade_t_account_statement_recycle_detail_full_1d.sql"


    )


    val source = Source.fromFile(sqlFile, "UTF-8")
    val rawFileText = try source.mkString finally source.close()
    val outFileSuffix = LocalDate.now().minusDays(1).toString
    val fileText = rawFileText.replace("${outFileSuffix}", outFileSuffix)
    val statements = splitStatements(fileText)
    val setupSqls = statements.filter(isSetupStatement)
    val sqlText = statements.find { stmt =>
      val upper = stmt.trim.toUpperCase
      upper.startsWith("INSERT ") ||
        upper.startsWith("WITH ") ||
        upper.startsWith("SELECT ") ||
        upper.startsWith("CREATE TABLE ") ||
        upper.startsWith("CREATE VIEW ")
    }.getOrElse {
      throw new IllegalArgumentException(s"No lineage SQL found in ${sqlFile.getAbsolutePath}")
    }

    setupSqls.foreach(sparkSession.sql)

    // scalastyle:off println
    println(s"=== SQL file: ${sqlFile.getAbsolutePath} ===")
    println(s"=== outFileSuffix: $outFileSuffix ===")
    println(s"=== graphType: $graphType ===")
    println("=== graphType mapping: 1=full, 2=contracted, 3=direct-table-field ===")
    println(s"=== contracted: $contracted ===")
    println("=== SQL ===")
    println(sqlText)
    println()
    // scalastyle:on println

    val parsed = sparkSession.sessionState.sqlParser.parsePlan(sqlText)
    // scalastyle:off println
    println("=== Parsed Plan ===")
    println(parsed.treeString)
    // scalastyle:on println

    Try {
      val analyzed = sparkSession.sessionState.analyzer.execute(parsed)
      sparkSession.sessionState.analyzer.checkAnalysis(analyzed)
      analyzed
    } match {
      case Success(analyzed) =>
        // scalastyle:off println
        println("=== Analyzed Plan ===")
        println(analyzed.treeString)
        val lineagePlan = resolveLineagePlan(analyzed)
        val lineageInputPlan = lineagePlan.inputPlan
        println(s"=== Lineage Input Plan Class: ${lineageInputPlan.getClass.getName} ===")
        println(lineageInputPlan.treeString)
        lineagePlan.materializedTarget.foreach { target =>
          println(s"=== Materialized Target === ${target.node.ident}")
        }
        println("=== SQLFlow ===")
        val sqlFlow = graphType match {
          case GraphType.Contracted => SQLContractedFlow()
          case _ => SQLFlow()
        }
        val (baseNodes, baseEdges) = sqlFlow.planToSQLFlow(lineageInputPlan)
        baseNodes.find(_.tpe == GraphNodeType.QueryNode).foreach { queryNode =>
          val incomingEdgesToQuery = baseEdges.filter(_.toId == queryNode.uniqueId)
          println(s"=== Query Node === ${queryNode.uniqueId} (${queryNode.ident})")
          println(s"=== Base Node Count === ${baseNodes.size}")
          println(s"=== Base Edge Count === ${baseEdges.size}")
          println(s"=== Incoming Edges To Query === ${incomingEdgesToQuery.size}")
          incomingEdgesToQuery.take(50).foreach { edge =>
            println(s"  $edge")
          }
        }
        val (nodes, edges) = attachMaterializedTarget(
          baseNodes,
          baseEdges,
          lineagePlan.materializedTarget)
        val graphSink = GraphVizSink()
        val metadataGraphSink = GraphVizMetadataSink()
        val neo4jSink = Neo4jAuraFieldLineageSink(neo4jUri, neo4jUser, neo4jPasswd)
        val modeDirName = s"type-$graphType"
        val outputDir = new File(s"target/sqlflow-debug/$modeDirName/basic").getAbsolutePath
        val metadataOutputDir = new File(s"target/sqlflow-debug/$modeDirName/metadata").getAbsolutePath
        println(graphSink.toGraphString(nodes, edges))
        graphSink.write(nodes, edges, Map(
          "outputDirPath" -> outputDir,
          "overwrite" -> "true"
        ))
        metadataGraphSink.write(nodes, edges, Map(
          "outputDirPath" -> metadataOutputDir,
          "overwrite" -> "true"
        ))
        val neo4jOptions = Map(
          "graphMode" -> (graphType match {
            case GraphType.DirectTableField => "direct_table_field"
            case _ => "full"
          }),
          "sqlFileName" -> sqlFile.getName)
        neo4jSink.append(nodes, edges, neo4jOptions)
        println(s"=== SQLFlow DOT written to: $outputDir/sqlflow.dot ===")
        println(s"=== SQLFlow SVG path (if generated): $outputDir/sqlflow.svg ===")
        println(s"=== SQLFlow metadata DOT written to: $metadataOutputDir/sqlflow.dot ===")
        println(s"=== SQLFlow metadata SVG path (if generated): $metadataOutputDir/sqlflow.svg ===")
        println(s"=== SQLFlow appended into Neo4j: $neo4jUri ===")
        // scalastyle:on println

      case Failure(err) =>
        // scalastyle:off println
        println("=== Analyze Failed ===")
        println(err.getClass.getName)
        println(err.getMessage)
        // scalastyle:on println
    }
  }

  private def splitStatements(sqlText: String): Seq[String] = {
    sqlText
      .linesIterator
      .filterNot(_.trim.startsWith("--"))
      .mkString("\n")
      .split("(?<=[^\\\\]);")
      .map(_.trim)
      .filter(_.nonEmpty)
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
        Some(buildMaterializedTarget(ctas.tableDesc, GraphNodeType.TableNode, ctas.outputColumnNames)))

    case ctas: CreateDataSourceTableAsSelectCommand =>
      LineagePlan(
        ctas.query,
        Some(buildMaterializedTarget(ctas.table, GraphNodeType.TableNode, ctas.outputColumnNames)))

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
