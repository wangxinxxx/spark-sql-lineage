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

package org.apache.spark.api.python

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.neo4j.driver.{AuthTokens, Config, Driver, GraphDatabase, Session, Value, Values}

object Neo4jUpstreamSummaryDemo {

  private val DefaultFieldName = "gold_zz_net_profit"

  private case class CliConfig(
      uri: String = "bolt://localhost:7687",
      user: String = "neo4j",
      passwd: String = "wx123456..",
      targetName: Option[String] = None,
      targetUid: Option[String] = None,
      fieldName: Option[String] = Some(DefaultFieldName))

  private case class TargetNode(
      uid: String,
      name: String,
      labels: Seq[String],
      attributeNames: Seq[String])

  private case class UpstreamNode(uid: String, name: String, label: String, minDepth: Int)

  private case class DependencyPath(hopCount: Int, nodes: Seq[String])

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args.toList)
    withSession(config) { session =>
      val targets = resolveTargets(session, config)
      printMatchedTargets(targets, config)
      targets.zipWithIndex.foreach { case (target, index) =>
        val directUpstreams = fetchDirectUpstreams(session, target.uid)
        val allUpstreams = fetchAllUpstreams(session, target.uid)
        val hierarchyPaths = fetchHierarchyPaths(session, target.uid)
        val supportsFieldLineage = hasFieldLineageGraph(session)
        val fieldPaths = if (supportsFieldLineage) {
          fetchFieldPaths(session, target.uid, config.fieldName)
        } else {
          Nil
        }
        printSummary(
          target,
          config.fieldName,
          directUpstreams,
          allUpstreams,
          hierarchyPaths,
          supportsFieldLineage,
          fieldPaths,
          index,
          targets.size)
      }
    }
  }

  private def parseArgs(args: List[String]): CliConfig = {
    @tailrec
    def loop(
        rest: List[String],
        config: CliConfig,
        positionalArgs: List[String]): CliConfig = rest match {
      case Nil =>
        validateConfig(mergePositionalArgs(config, positionalArgs))

      case "--help" :: _ =>
        printUsageAndExit()
        config

      case "--uri" :: value :: tail =>
        loop(tail, config.copy(uri = value), positionalArgs)

      case "--user" :: value :: tail =>
        loop(tail, config.copy(user = value), positionalArgs)

      case "--passwd" :: value :: tail =>
        loop(tail, config.copy(passwd = value), positionalArgs)

      case "--name" :: value :: tail =>
        loop(tail, config.copy(targetName = Some(value)), positionalArgs)

      case "--uid" :: value :: tail =>
        loop(tail, config.copy(targetUid = Some(value)), positionalArgs)

      case "--field" :: value :: tail =>
        loop(tail, config.copy(fieldName = Some(value)), positionalArgs)

      case option :: _ if option.startsWith("--") =>
        throw new IllegalArgumentException(s"Unknown option: $option")

      case value :: tail =>
        loop(tail, config, positionalArgs :+ value)
    }

    loop(args, CliConfig(), Nil)
  }

  private def mergePositionalArgs(config: CliConfig, positionalArgs: Seq[String]): CliConfig = {
    positionalArgs match {
      case Nil =>
        config
      case Seq(name) if config.targetName.isEmpty && config.targetUid.isEmpty =>
        config.copy(targetName = Some(name))
      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected positional arguments: ${positionalArgs.mkString(" ")}")
    }
  }

  private def validateConfig(config: CliConfig): CliConfig = {
    if (config.targetName.isDefined && config.targetUid.isDefined) {
      throw new IllegalArgumentException(
        "Specify at most one of --name <nodeName> or --uid <nodeUid>")
    }
    config
  }

  private def printUsageAndExit(): Unit = {
    // scalastyle:off println
    println(
      """Usage:
        |  Neo4jUpstreamSummaryDemo --name <nodeName> [--field <fieldName>]
        |  Neo4jUpstreamSummaryDemo --uid <nodeUid> [--field <fieldName>]
        |  Neo4jUpstreamSummaryDemo [--field <fieldName>]
        |
        |Options:
        |  --uri <boltUri>     Neo4j bolt uri, default: bolt://localhost:7687
        |  --user <user>       Neo4j user, default: neo4j
        |  --passwd <passwd>   Neo4j password, default: wx123456..
        |  --name <nodeName>   Target node name
        |  --uid <nodeUid>     Target node uid
        |  --field <field>     Field filter, default: gold_zz_net_profit
        |
        |Examples:
        |  Neo4jUpstreamSummaryDemo
        |  Neo4jUpstreamSummaryDemo --field gold_zz_net_profit
        |  Neo4jUpstreamSummaryDemo --name tmp_recycle_gold_order_data_v3
        |  Neo4jUpstreamSummaryDemo --name tmp_recycle_gold_order_data_v3 \
        |    --field gold_zz_net_profit
        |  Neo4jUpstreamSummaryDemo --uid <targetUid>
        |""".stripMargin)
    // scalastyle:on println
    sys.exit(0)
  }

  private def withSession[T](config: CliConfig)(f: Session => T): T = {
    var driver: Driver = null
    var session: Session = null
    try {
      driver = GraphDatabase.driver(
        config.uri,
        AuthTokens.basic(config.user, config.passwd),
        Config.defaultConfig())
      session = driver.session()
      f(session)
    } finally {
      if (session != null) {
        session.close()
      }
      if (driver != null) {
        driver.close()
      }
    }
  }

  private def resolveTargets(session: Session, config: CliConfig): Seq[TargetNode] = {
    val query =
      """
        |MATCH (n)
        |WHERE (
        |    ($uid <> '' AND n.uid = $uid)
        | OR ($name <> '' AND n.name = $name)
        | OR (
        |      $uid = '' AND $name = '' AND $field <> '' AND
        |      $field IN coalesce(n.attributeNames, [])
        |    )
        |)
        |AND ($field = '' OR $field IN coalesce(n.attributeNames, []))
        |RETURN labels(n) AS labels,
        |       n.name AS name,
        |       n.uid AS uid,
        |       coalesce(n.attributeNames, []) AS attributeNames
        |ORDER BY head(labels(n)), name, uid
        |""".stripMargin
    val params = Values.parameters(
      "uid", config.targetUid.getOrElse(""),
      "name", config.targetName.getOrElse(""),
      "field", config.fieldName.getOrElse(""))
    val targets = session.run(query, params).list().asScala.map { record =>
      TargetNode(
        uid = record.get("uid").asString(),
        name = record.get("name").asString(),
        labels = toStringSeq(record.get("labels")),
        attributeNames = toStringSeq(record.get("attributeNames")))
    }.toSeq

    if (targets.isEmpty) {
      val targetDesc = config.targetUid
        .map(uid => s"uid=$uid")
        .orElse(config.targetName.map(name => s"name=$name"))
        .orElse(config.fieldName.map(field => s"field=$field"))
        .getOrElse("unknown target")
      throw new IllegalArgumentException(s"Target node not found: $targetDesc")
    }

    if (config.targetUid.isDefined && targets.size > 1) {
      val candidates = targets.map { target =>
        s"${target.labels.mkString(":")}:${target.name} (uid=${target.uid})"
      }.mkString("\n")
      throw new IllegalArgumentException(
        s"Matched multiple target nodes for uid, please inspect data:\n$candidates")
    }

    targets
  }

  private def printMatchedTargets(targets: Seq[TargetNode], config: CliConfig): Unit = {
    val selector = config.targetUid
      .map(uid => s"uid=$uid")
      .orElse(config.targetName.map(name => s"name=$name"))
      .orElse(config.fieldName.map(field => s"field=$field"))
      .getOrElse("default selector")

    // scalastyle:off println
    println("=== Matched Target Nodes ===")
    println(s"selector: $selector")
    println(s"matched target count: ${targets.size}")
    targets.zipWithIndex.foreach { case (target, index) =>
      println(
        s"${index + 1}. ${target.labels.mkString(":")}:${target.name}\tuid=${target.uid}")
    }
    println()
    // scalastyle:on println
  }

  private def validateFieldFilter(target: TargetNode, fieldName: Option[String]): Unit = {
    fieldName.foreach { field =>
      if (!target.attributeNames.contains(field)) {
        throw new IllegalArgumentException(
          s"Field `$field` does not exist on target node `${target.name}`")
      }
    }
  }

  private def fetchDirectUpstreams(session: Session, targetUid: String): Seq[UpstreamNode] = {
    val query =
      """
        |MATCH (dst {uid: $uid})
        |OPTIONAL MATCH (src)-[:transformInto]->(dst)
        |WITH DISTINCT src
        |WHERE src IS NOT NULL
        |RETURN src.uid AS uid,
        |       src.name AS name,
        |       head(labels(src)) AS label,
        |       1 AS minDepth
        |ORDER BY label, name
        |""".stripMargin
    val params = Values.parameters("uid", targetUid)
    session.run(query, params).list().asScala.map(toUpstreamNode).toSeq
  }

  private def fetchAllUpstreams(session: Session, targetUid: String): Seq[UpstreamNode] = {
    val query =
      """
        |MATCH (dst {uid: $uid})
        |OPTIONAL MATCH p=(src)-[:transformInto*]->(dst)
        |WHERE src.uid <> dst.uid
        |WITH src, min(length(p)) AS minDepth
        |WHERE src IS NOT NULL
        |RETURN src.uid AS uid,
        |       src.name AS name,
        |       head(labels(src)) AS label,
        |       minDepth AS minDepth
        |ORDER BY minDepth, label, name
        |""".stripMargin
    val params = Values.parameters("uid", targetUid)
    session.run(query, params).list().asScala.map(toUpstreamNode).toSeq
  }

  private def fetchHierarchyPaths(session: Session, targetUid: String): Seq[DependencyPath] = {
    val query =
      """
        |MATCH p=(src)-[:transformInto*]->(dst {uid: $uid})
        |WHERE src.uid <> dst.uid
        |  AND NOT EXISTS {
        |    MATCH (prev)-[:transformInto]->(src)
        |  }
        |WITH DISTINCT length(p) AS hopCount,
        |     [n IN nodes(p) | head(labels(n)) + ":" + n.name] AS path
        |RETURN hopCount, path
        |ORDER BY hopCount, size(path)
        |LIMIT 50
        |""".stripMargin
    val params = Values.parameters("uid", targetUid)
    session.run(query, params).list().asScala.map(toDependencyPath).toSeq
  }

  private def hasFieldLineageGraph(session: Session): Boolean = {
    val query =
      """
        |MATCH (:Field)-[:DERIVES_TO]->(:Field)
        |RETURN count(*) > 0 AS supported
        |LIMIT 1
        |""".stripMargin
    val record = session.run(query).single()
    record.get("supported").asBoolean()
  }

  private def fetchFieldPaths(
      session: Session,
      targetUid: String,
      fieldName: Option[String]): Seq[DependencyPath] = {
    val normalizedFieldName = fieldName.getOrElse("")
    if (normalizedFieldName.isEmpty) {
      Nil
    } else {
      val query =
        """
          |MATCH (owner {uid: $uid})-[:HAS_FIELD]->(dstField:Field {name: $field})
          |MATCH p=(srcField:Field)-[:DERIVES_TO*]->(dstField)
          |WHERE srcField.uid <> dstField.uid
          |  AND NOT EXISTS {
          |    MATCH (:Field)-[:DERIVES_TO]->(srcField)
          |  }
          |WITH DISTINCT length(p) AS hopCount,
          |     [n IN nodes(p) |
          |       coalesce(n.ownerLabel, "Field") + ":" +
          |       coalesce(n.ownerName, "?") + "." + n.name] AS path
          |RETURN hopCount, path
          |ORDER BY hopCount, size(path)
          |LIMIT 50
          |""".stripMargin
      val params = Values.parameters("uid", targetUid, "field", normalizedFieldName)
      session.run(query, params).list().asScala.map(toDependencyPath).toSeq
    }
  }

  private def printSummary(
      target: TargetNode,
      fieldName: Option[String],
      directUpstreams: Seq[UpstreamNode],
      allUpstreams: Seq[UpstreamNode],
      hierarchyPaths: Seq[DependencyPath],
      supportsFieldLineage: Boolean,
      fieldPaths: Seq[DependencyPath],
      targetIndex: Int,
      totalTargets: Int): Unit = {
    validateFieldFilter(target, fieldName)
    val labelCounts = allUpstreams.groupBy(_.label).toSeq.sortBy { case (label, _) =>
      labelOrder(label)
    }
    val businessUpstreams = allUpstreams.filter { node =>
      node.label == "Table" || node.label == "View"
    }
    val sampledColumns = abbreviate(target.attributeNames, 12)

    // scalastyle:off println
    println(s"=== Target Summary ${targetIndex + 1}/$totalTargets ===")
    println(s"name: ${target.name}")
    println(s"uid: ${target.uid}")
    println(s"labels: ${target.labels.mkString(", ")}")
    println(s"columns: $sampledColumns")
    fieldName.foreach { field =>
      println(s"field filter: $field")
    }
    println()

    println("=== Upstream Summary ===")
    println(s"direct upstream count: ${directUpstreams.size}")
    println(s"all upstream count: ${allUpstreams.size}")
    println(
      s"max hop distance: ${allUpstreams.map(_.minDepth).reduceOption(_ max _).getOrElse(0)}")
    labelCounts.foreach { case (label, nodes) =>
      println(s"$label: ${nodes.size}")
    }
    println()

    println("=== Upstream By Hop ===")
    printHopCounts(allUpstreams)
    println()

    println("=== Hierarchy Paths ===")
    printDependencyPaths(hierarchyPaths)
    println()

    println("=== Field Lineage ===")
    if (fieldName.isEmpty) {
      println("(field filter not specified)")
    } else if (!supportsFieldLineage) {
      println("current Neo4j graph only stores node-level transformInto relationships.")
      println("field-level mapping was not persisted, so exact field dependencies")
      println("cannot be reconstructed by query alone from the current database.")
    } else {
      printDependencyPaths(fieldPaths)
    }
    println()

    println("=== Direct Upstream Nodes ===")
    printNodeList(directUpstreams)
    println()

    println("=== Upstream Tables And Views ===")
    printNodeList(businessUpstreams, includeUid = false)
    println()

    println("=== All Upstream Nodes ===")
    printNodeList(allUpstreams)
    println()
    // scalastyle:on println
  }

  private def toUpstreamNode(record: org.neo4j.driver.Record): UpstreamNode = {
    UpstreamNode(
      uid = record.get("uid").asString(),
      name = record.get("name").asString(),
      label = record.get("label").asString(),
      minDepth = record.get("minDepth").asInt())
  }

  private def toDependencyPath(record: org.neo4j.driver.Record): DependencyPath = {
    DependencyPath(
      hopCount = record.get("hopCount").asInt(),
      nodes = toStringSeq(record.get("path")))
  }

  private def toStringSeq(value: Value): Seq[String] = {
    if (value == null || value.isNull) {
      Nil
    } else {
      value.asList((v: Value) => v.asString()).asScala.toSeq
    }
  }

  private def printHopCounts(nodes: Seq[UpstreamNode]): Unit = {
    // scalastyle:off println
    if (nodes.isEmpty) {
      println("(none)")
    } else {
      nodes.groupBy(_.minDepth).toSeq.sortBy(_._1).foreach { case (depth, groupedNodes) =>
        val summary = groupedNodes.groupBy(_.label).toSeq.sortBy { case (label, _) =>
          labelOrder(label)
        }.map { case (label, sameLabelNodes) =>
          s"$label=${sameLabelNodes.size}"
        }.mkString(", ")
        println(s"hop $depth: ${groupedNodes.size} nodes ($summary)")
      }
    }
    // scalastyle:on println
  }

  private def printNodeList(nodes: Seq[UpstreamNode], includeUid: Boolean = true): Unit = {
    // scalastyle:off println
    if (nodes.isEmpty) {
      println("(none)")
    } else {
      nodes.groupBy(_.minDepth).toSeq.sortBy(_._1).foreach { case (depth, groupedNodes) =>
        println(s"hop $depth")
        groupedNodes.sortBy(node => (labelOrder(node.label), node.name)).foreach { node =>
          val uidSuffix = if (includeUid) s" [uid=${node.uid}]" else ""
          println(s"  - [${node.label}] ${node.name}$uidSuffix")
        }
      }
    }
    // scalastyle:on println
  }

  private def printDependencyPaths(paths: Seq[DependencyPath]): Unit = {
    // scalastyle:off println
    if (paths.isEmpty) {
      println("(none)")
    } else {
      paths.zipWithIndex.foreach { case (path, index) =>
        println(s"path ${index + 1} (${path.hopCount} hops)")
        println(s"  ${path.nodes.mkString(" -> ")}")
      }
    }
    // scalastyle:on println
  }

  private def labelOrder(label: String): Int = label match {
    case "Table" => 0
    case "View" => 1
    case "LeafPlan" => 2
    case "Plan" => 3
    case "Query" => 4
    case _ => 5
  }

  private def abbreviate(values: Seq[String], limit: Int): String = {
    if (values.isEmpty) {
      "(none)"
    } else if (values.size <= limit) {
      values.mkString(", ")
    } else {
      s"${values.take(limit).mkString(", ")}, ... (${values.size} total)"
    }
  }
}
