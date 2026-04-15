/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.flow.sink

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.neo4j.driver._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.flow._

case class Neo4jFieldLineageWriteStats(nodeCount: Int, edgeCount: Int)

case class Neo4jAuraFieldLineageSink(uri: String, user: String, passwd: String)
  extends BaseGraphBatchSink with BaseGraphStreamSink with Neo4jAura with Logging {

  private object GraphMode extends Enumeration {
    val Full, DirectTableField = Value
  }

  private case class NodeRef(
      node: SQLFlowGraphNode,
      label: String,
      matchPredicate: String,
      fieldOwnerKey: String)

  private case class FieldRef(
      uid: String,
      name: String,
      port: Int,
      ownerName: String,
      ownerLabel: String,
      outputExpression: Option[String])

  override def toString: String = {
    s"${this.getClass.getSimpleName}(uri=$uri, user=$user)"
  }

  private def genLabel(n: SQLFlowGraphNode): String = n.tpe match {
    case GraphNodeType.TableNode => "Table"
    case GraphNodeType.ViewNode => "View"
    case GraphNodeType.PlanNode => "Plan"
    case GraphNodeType.LeafPlanNode => "LeafPlan"
    case GraphNodeType.QueryNode => "Query"
  }

  private def genProps(n: SQLFlowGraphNode): java.util.Map[String, Object] = {
    val basicProps: Map[String, Object] = Map(
      "name" -> n.ident,
      "uid" -> n.uniqueId,
      "attributeNames" -> n.attributeNames.asJava,
      "schemaDDL" -> n.schemaDDL)
    val allProps = basicProps ++ n.props.map { case (k, v) =>
      k -> v.asInstanceOf[Object]
    }
    allProps.asJava
  }

  private def buildNodeRef(n: SQLFlowGraphNode): NodeRef = {
    val label = genLabel(n)
    val matchPredicate = n.tpe match {
      case GraphNodeType.PlanNode | GraphNodeType.LeafPlanNode =>
        "semanticHash = \"" + n.props("semanticHash") + "\""
      case _ =>
        "uid = \"" + n.uniqueId + "\""
    }
    val fieldOwnerKey = n.tpe match {
      case GraphNodeType.PlanNode | GraphNodeType.LeafPlanNode =>
        n.props("semanticHash")
      case _ =>
        n.uniqueId
    }
    NodeRef(n, label, matchPredicate, fieldOwnerKey)
  }

  private def buildNodeMap(nodes: Seq[SQLFlowGraphNode]): Map[String, NodeRef] = {
    nodes.map { n => n.uniqueId -> buildNodeRef(n) }.toMap
  }

  private def buildFieldRefs(nodeMap: Map[String, NodeRef]): Seq[FieldRef] = {
    nodeMap.values.flatMap { nodeRef =>
      nodeRef.node.attributeNames.indices.flatMap { port =>
        buildFieldRef(nodeRef, port)
      }
    }.toSeq
  }

  private def buildFieldRef(nodeRef: NodeRef, port: Int): Option[FieldRef] = {
    nodeRef.node.attributeNames.lift(port).map { fieldName =>
      FieldRef(
        s"${nodeRef.fieldOwnerKey}#$port",
        fieldName,
        port,
        nodeRef.node.ident,
        nodeRef.label,
        nodeRef.node.props.get(s"${SQLFlowGraphProps.OutputExpressionByIndexPrefix}$port"))
    }
  }

  private def isTableLikeOwner(ownerLabel: String): Boolean = {
    ownerLabel == "Table" || ownerLabel == "View"
  }

  private def tableFieldNodeCount(nodeMap: Map[String, NodeRef]): Int = {
    buildFieldRefs(nodeMap).count(ref => isTableLikeOwner(ref.ownerLabel))
  }

  private def collectTargetTableFieldUids(
      nodeMap: Map[String, NodeRef],
      edges: Seq[SQLFlowGraphEdge]): Set[String] = {
    edges.flatMap { edge =>
      for {
        fromIdx <- edge.fromIdx
        toIdx <- edge.toIdx
        fromNodeRef <- nodeMap.get(edge.fromId)
        toNodeRef <- nodeMap.get(edge.toId)
        fromFieldRef <- buildFieldRef(fromNodeRef, fromIdx)
        toFieldRef <- buildFieldRef(toNodeRef, toIdx)
        if fromFieldRef.ownerLabel == "Query" && isTableLikeOwner(toFieldRef.ownerLabel)
      } yield {
        toFieldRef.uid
      }
    }.toSet
  }

  private def directTableFieldPairs(
      nodeMap: Map[String, NodeRef],
      edges: Seq[SQLFlowGraphEdge]): Seq[(String, String)] = {
    val mappedFieldEdges = edges.flatMap { edge =>
      for {
        fromIdx <- edge.fromIdx
        toIdx <- edge.toIdx
        fromNodeRef <- nodeMap.get(edge.fromId)
        toNodeRef <- nodeMap.get(edge.toId)
        fromFieldRef <- buildFieldRef(fromNodeRef, fromIdx)
        toFieldRef <- buildFieldRef(toNodeRef, toIdx)
      } yield {
        fromFieldRef.uid -> toFieldRef.uid
      }
    }
    val reverseAdj = mappedFieldEdges.groupBy(_._2).map { case (toUid, pairs) =>
      toUid -> pairs.map(_._1).distinct
    }
    val fieldRefByUid = buildFieldRefs(nodeMap).map { fieldRef =>
      fieldRef.uid -> fieldRef
    }.toMap
    val targetTableFieldUids = collectTargetTableFieldUids(nodeMap, edges)

    def collectUpstreamTableFieldUids(targetFieldUid: String): Seq[String] = {
      val visited = mutable.Set[String](targetFieldUid)
      val pending = mutable.Queue[String](targetFieldUid)
      val upstreamTableFields = mutable.LinkedHashSet[String]()
      while (pending.nonEmpty) {
        val current = pending.dequeue()
        reverseAdj.getOrElse(current, Nil).foreach { prevUid =>
          if (!visited.contains(prevUid)) {
            visited += prevUid
            fieldRefByUid.get(prevUid).foreach { prevFieldRef =>
              if (isTableLikeOwner(prevFieldRef.ownerLabel) && prevUid != targetFieldUid) {
                upstreamTableFields += prevUid
              }
            }
            pending.enqueue(prevUid)
          }
        }
      }
      upstreamTableFields.toSeq
    }

    targetTableFieldUids.toSeq.flatMap { targetFieldUid =>
      collectUpstreamTableFieldUids(targetFieldUid).map { upstreamFieldUid =>
        upstreamFieldUid -> targetFieldUid
      }
    }.distinct
  }

  def plannedWriteStats(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Neo4jFieldLineageWriteStats = {
    graphMode(options) match {
      case GraphMode.DirectTableField =>
        val nodeMap = buildNodeMap(nodes)
        Neo4jFieldLineageWriteStats(
          tableFieldNodeCount(nodeMap),
          directTableFieldPairs(nodeMap, edges).size)
      case GraphMode.Full =>
        Neo4jFieldLineageWriteStats(nodes.size, edges.size)
    }
  }

  private def appendSqlFileNameSql(enabled: Boolean): String = {
    if (enabled) {
      """
        |SET field.sqlFileNames = CASE
        |  WHEN $sqlFileName IS NULL THEN coalesce(field.sqlFileNames, [])
        |  WHEN $sqlFileName IN coalesce(field.sqlFileNames, [])
        |    THEN coalesce(field.sqlFileNames, [])
        |  ELSE coalesce(field.sqlFileNames, []) + $sqlFileName
        |END
      """.stripMargin
    } else {
      ""
    }
  }

  private def graphMode(options: Map[String, String]): GraphMode.Value = {
    options.get("graphMode").map(_.trim.toLowerCase(Locale.ROOT)) match {
      case Some("direct_table_field") => GraphMode.DirectTableField
      case _ => GraphMode.Full
    }
  }

  private def enableDirectTableFieldLineage(options: Map[String, String]): Boolean = {
    graphMode(options) == GraphMode.DirectTableField ||
      options.getOrElse("enableDirectTableFieldLineage", "false").toBoolean
  }

  private def tryToCreateConstraints(s: Session): Unit = try {
    def genCreateConstraintStmt(label: String, uniqProp: String): String = {
      s"""
         |CREATE CONSTRAINT unique_${label.toLowerCase}_node_constraint IF NOT EXISTS
         |FOR (n:$label)
         |REQUIRE n.$uniqProp IS UNIQUE
       """.stripMargin
    }
    withTx(s) { tx =>
      tx.run(genCreateConstraintStmt("Table", "uid"))
      tx.run(genCreateConstraintStmt("View", "uid"))
      tx.run(genCreateConstraintStmt("Query", "uid"))
      tx.run(genCreateConstraintStmt("Plan", "semanticHash"))
      tx.run(genCreateConstraintStmt("LeafPlan", "semanticHash"))
      tx.run(genCreateConstraintStmt("Field", "uid"))
    }
  } catch {
    case NonFatal(_) =>
  }

  private def tryToCreateNodes(s: Session, nodes: Seq[SQLFlowGraphNode]): Unit = {
    withTx(s) { tx =>
      createNodes(tx, nodes)
    }
  }

  private def createNodes(tx: Transaction, nodes: Seq[SQLFlowGraphNode]): Unit = {
    nodes.foreach { n =>
      val label = genLabel(n)
      val props = genProps(n)
      n.tpe match {
        case GraphNodeType.PlanNode | GraphNodeType.LeafPlanNode =>
          val semanticHash = n.props.getOrElse("semanticHash", {
            throw new AnalysisException(
              s"Missing semanticHash for plan node '${n.uniqueId}'")
          })
          tx.run(
            s"""
               |MERGE (node:$label {semanticHash: $$semanticHash})
               |SET node += $$props
             """.stripMargin,
            Values.parameters(
              "semanticHash", semanticHash,
              "props", props))
        case _ =>
          tx.run(
            s"""
               |MERGE (node:$label {uid: $$uid})
               |SET node += $$props
             """.stripMargin,
            Values.parameters(
              "uid", n.uniqueId,
              "props", props))
      }
    }
  }

  private def createFieldNodes(
      tx: Transaction,
      nodeMap: Map[String, NodeRef],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    val sqlFileName = options.get("sqlFileName").orNull
    val targetFieldUids = collectTargetTableFieldUids(nodeMap, edges)
    nodeMap.values.foreach { nodeRef =>
      nodeRef.node.attributeNames.zipWithIndex.foreach { case (_, port) =>
        buildFieldRef(nodeRef, port).foreach { fieldRef =>
          val fieldProps: Map[String, Object] = {
            val basicProps: Map[String, Object] = Map(
              "name" -> fieldRef.name,
              "port" -> Int.box(fieldRef.port),
              "ownerName" -> fieldRef.ownerName,
              "ownerLabel" -> fieldRef.ownerLabel)
            val withTableName =
              if (isTableLikeOwner(fieldRef.ownerLabel)) {
                basicProps + ("tableName" -> fieldRef.ownerName)
              } else {
                basicProps
              }
            fieldRef.outputExpression.map { expr =>
              withTableName + ("outputExpression" -> expr)
            }.getOrElse(withTableName)
          }
          val taskPropsSql = appendSqlFileNameSql(targetFieldUids.contains(fieldRef.uid))
          tx.run(
            s"""
               |MATCH (owner:${nodeRef.label})
               |WHERE owner.${nodeRef.matchPredicate}
               |MERGE (field:Field {uid: $$fieldUid})
               |SET field += $$fieldProps
               |$taskPropsSql
               |MERGE (owner)-[:HAS_FIELD]->(field)
             """.stripMargin,
            Values.parameters(
              "fieldUid", fieldRef.uid,
              "fieldProps", fieldProps.asJava,
              "sqlFileName", sqlFileName))
        }
      }
    }
  }

  private def createTableFieldOnlyNodes(
      tx: Transaction,
      nodeMap: Map[String, NodeRef],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    val sqlFileName = options.get("sqlFileName").orNull
    val targetFieldUids = collectTargetTableFieldUids(nodeMap, edges)
    nodeMap.values.filter(nodeRef => isTableLikeOwner(nodeRef.label)).foreach { nodeRef =>
      nodeRef.node.attributeNames.zipWithIndex.foreach { case (_, port) =>
        buildFieldRef(nodeRef, port).foreach { fieldRef =>
          val fieldProps: Map[String, Object] = Map(
            "name" -> fieldRef.name,
            "port" -> Int.box(fieldRef.port),
            "ownerName" -> fieldRef.ownerName,
            "ownerLabel" -> fieldRef.ownerLabel,
            "tableName" -> fieldRef.ownerName)
          tx.run(
            s"""
               |MERGE (field:Field {uid: $$fieldUid})
               |SET field += $$fieldProps
               |${appendSqlFileNameSql(targetFieldUids.contains(fieldRef.uid))}
             """.stripMargin,
            Values.parameters(
              "fieldUid", fieldRef.uid,
              "fieldProps", fieldProps.asJava,
              "sqlFileName", sqlFileName))
        }
      }
    }
  }

  private def createFieldEdges(
      tx: Transaction,
      nodeMap: Map[String, NodeRef],
      edges: Seq[SQLFlowGraphEdge]): Unit = {
    val fieldEdgeProps = edges.flatMap { edge =>
      for {
        fromIdx <- edge.fromIdx
        toIdx <- edge.toIdx
        fromNodeRef <- nodeMap.get(edge.fromId)
        toNodeRef <- nodeMap.get(edge.toId)
        fromFieldRef <- buildFieldRef(fromNodeRef, fromIdx)
        toFieldRef <- buildFieldRef(toNodeRef, toIdx)
      } yield {
        ((fromFieldRef.uid, toFieldRef.uid), edge.props.toMap)
      }
    }.groupBy(_._1).map { case (fieldPair, entries) =>
      val mergedProps = mutable.LinkedHashMap[String, String]()
      entries.foreach { case (_, props) =>
        props.foreach { case (k, v) => mergedProps(k) = v }
      }
      fieldPair -> mergedProps.toMap
    }.toSeq

    fieldEdgeProps.foreach { case ((fromFieldUid, toFieldUid), props) =>
      tx.run(
        """
          |MATCH (src:Field {uid: $srcFieldUid}), (dst:Field {uid: $dstFieldUid})
          |MERGE (src)-[r:DERIVES_TO]->(dst)
          |SET r += $props
        """.stripMargin,
        Values.parameters(
          "srcFieldUid", fromFieldUid,
          "dstFieldUid", toFieldUid,
          "props", props.map { case (k, v) => k -> v.asInstanceOf[Object] }.asJava))
    }
  }

  private def createDirectTableFieldEdges(
      tx: Transaction,
      nodeMap: Map[String, NodeRef],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    val sqlFileName = options.get("sqlFileName").orNull
    val directLineagePairs = directTableFieldPairs(nodeMap, edges)

    directLineagePairs.foreach { case (srcFieldUid, dstFieldUid) =>
      tx.run(
        """
          |MATCH (src:Field {uid: $srcFieldUid}), (dst:Field {uid: $dstFieldUid})
          |MERGE (src)-[r:DIRECT_DERIVES_TO]->(dst)
          |SET r.sqlFileNames = CASE
          |  WHEN $sqlFileName IS NULL THEN coalesce(r.sqlFileNames, [])
          |  WHEN $sqlFileName IN coalesce(r.sqlFileNames, []) THEN coalesce(r.sqlFileNames, [])
          |  ELSE coalesce(r.sqlFileNames, []) + $sqlFileName
          |END
        """.stripMargin,
        Values.parameters(
          "srcFieldUid", srcFieldUid,
          "dstFieldUid", dstFieldUid,
          "sqlFileName", sqlFileName))
    }
  }

  private def createEdges(
      tx: Transaction,
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    val nodeMap = buildNodeMap(nodes)
    val compactEdges = edges.map { e => (e.fromId, e.toId) }.distinct
    val edgeMap = compactEdges.groupBy(_._1).map { case (fromId, pairs) =>
      fromId -> pairs.map(_._2)
    }

    def collectDstNodeIds(fromId: String): Seq[String] = {
      val buf = mutable.ArrayBuffer[String]()
      val maxDepthToTraverse = 128
      var pending = Seq(fromId)
      (0 until maxDepthToTraverse).foreach { _ =>
        pending = pending.flatMap { nodeId =>
          edgeMap.getOrElse(nodeId, Nil).flatMap(nodeMap.get).flatMap { nodeRef =>
            nodeRef.node.tpe match {
              case GraphNodeType.QueryNode | GraphNodeType.ViewNode =>
                buf.append(nodeRef.node.uniqueId)
                None
              case _ =>
                Some(nodeRef.node.uniqueId)
            }
          }
        }

        if (pending.isEmpty) {
          return buf.distinct.toSeq
        }
      }
      buf.distinct.toSeq
    }

    createFieldNodes(tx, nodeMap, edges, options)
    createFieldEdges(tx, nodeMap, edges)
    if (enableDirectTableFieldLineage(options)) {
      createDirectTableFieldEdges(tx, nodeMap, edges, options)
    }

    compactEdges.foreach { case (fromId, toId) =>
      val fromNodeRef = nodeMap(fromId)
      val toNodeRef = nodeMap(toId)
      tx.run(
        s"""
           |MATCH (src:${fromNodeRef.label}), (dst:${toNodeRef.label})
           |WHERE src.${fromNodeRef.matchPredicate} AND dst.${toNodeRef.matchPredicate}
           |MERGE (src)-[r:transformInto]->(dst)
           |ON CREATE SET r.dstNodeIds = $$dstNodeIds
           |ON MATCH SET r.dstNodeIds = r.dstNodeIds + $$dstNodeIds
           |RETURN r.dstNodeIds
         """.stripMargin,
        Values.parameters("dstNodeIds", collectDstNodeIds(fromId).asJava))
    }
  }

  private def createDirectTableFieldOnlyGraph(
      tx: Transaction,
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    val nodeMap = buildNodeMap(nodes)
    createTableFieldOnlyNodes(tx, nodeMap, edges, options)
    createDirectTableFieldEdges(tx, nodeMap, edges, options)
  }

  private def isDatabaseEmpty(tx: Transaction): Boolean = {
    !tx.run("MATCH (n) RETURN 1 LIMIT 1").hasNext
  }

  override def write(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    val overwrite = options.getOrElse("overwrite", "false").toBoolean
    withSession { s =>
      if (!overwrite) {
        withTx(s) { tx =>
          if (!isDatabaseEmpty(tx)) {
            throw new AnalysisException("Database should be empty")
          }
        }
      } else {
        resetNeo4jDbState()
      }
      withTx(s) { tx =>
        graphMode(options) match {
          case GraphMode.DirectTableField =>
            createDirectTableFieldOnlyGraph(tx, nodes, edges, options)
          case GraphMode.Full =>
            createNodes(tx, nodes)
            createEdges(tx, nodes, edges, options)
        }
      }
    }
  }

  override def append(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = {
    withSession { s =>
      tryToCreateConstraints(s)
      if (graphMode(options) == GraphMode.Full) {
        tryToCreateNodes(s, nodes)
      }
      withTx(s) { tx =>
        graphMode(options) match {
          case GraphMode.DirectTableField =>
            createDirectTableFieldOnlyGraph(tx, nodes, edges, options)
          case GraphMode.Full =>
            createEdges(tx, nodes, edges, options)
        }
      }
    }
  }
}
