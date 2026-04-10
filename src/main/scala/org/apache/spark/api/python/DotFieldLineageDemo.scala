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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

object DotFieldLineageDemo {

  private val DefaultDotPath = "target/sqlflow-debug/sqlflow.dot"
  private val DefaultFieldName = "gold_zz_net_profit"
  private val DefaultMaxPaths = 50

  private val NodeStartPattern: Regex = "^\\s*\"([^\"]+)\" \\[label=<\\s*$".r
  private val NodeNamePattern: Regex = """.*port="nodeName">(.*)</td></tr>.*""".r
  private val FieldPattern: Regex = """.*<td port="(\d+)">(.*)</td></tr>.*""".r
  private val EdgePattern: Regex =
    """^\s*"([^"]+)":([^ ]+)\s*->\s*"([^"]+)":([^;]+);$""".r

  private case class CliConfig(
      dotPath: String = DefaultDotPath,
      fieldName: String = DefaultFieldName,
      nodeFilter: Option[String] = None,
      maxPaths: Int = DefaultMaxPaths)

  private case class DotNode(
      uniqueId: String,
      displayName: String,
      fieldsByPort: Map[Int, String])

  private case class DotEdge(
      srcId: String,
      srcPort: String,
      dstId: String,
      dstPort: String)

  private case class ParsedDot(
      nodes: Map[String, DotNode],
      edges: Seq[DotEdge],
      incomingFieldEdges: Map[FieldRef, Seq[FieldRef]],
      outgoingFieldEdges: Map[FieldRef, Seq[FieldRef]])

  private case class FieldRef(nodeId: String, port: Int)

  private case class FieldEndpoint(
      nodeId: String,
      nodeName: String,
      port: Int,
      fieldName: String)

  private case class FieldPath(nodes: Seq[FieldEndpoint]) {
    def hopCount: Int = math.max(nodes.size - 1, 0)
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args.toList)
    val parsedDot = parseDot(config.dotPath)
    val targets = resolveTargets(parsedDot, config)
    printMatchedTargets(config, targets)
    targets.zipWithIndex.foreach { case (target, index) =>
      val immediateUpstreams = collectImmediateUpstreams(parsedDot, target)
      val fieldPaths = collectFieldPaths(parsedDot, target, config.maxPaths)
      printTargetSummary(target, immediateUpstreams, fieldPaths, index, targets.size, config)
    }
  }

  private def parseArgs(args: List[String]): CliConfig = {
    @tailrec
    def loop(rest: List[String], config: CliConfig): CliConfig = rest match {
      case Nil =>
        validateConfig(config)

      case "--help" :: _ =>
        printUsageAndExit()
        config

      case "--dot" :: value :: tail =>
        loop(tail, config.copy(dotPath = value))

      case "--field" :: value :: tail =>
        loop(tail, config.copy(fieldName = value))

      case "--node" :: value :: tail =>
        loop(tail, config.copy(nodeFilter = Some(value)))

      case "--max-paths" :: value :: tail =>
        loop(tail, config.copy(maxPaths = value.toInt))

      case option :: _ if option.startsWith("--") =>
        throw new IllegalArgumentException(s"Unknown option: $option")

      case value :: Nil =>
        validateConfig(config.copy(fieldName = value))

      case other =>
        throw new IllegalArgumentException(
          s"Unexpected arguments: ${other.mkString(" ")}")
    }

    loop(args, CliConfig())
  }

  private def validateConfig(config: CliConfig): CliConfig = {
    if (config.maxPaths <= 0) {
      throw new IllegalArgumentException("--max-paths must be greater than 0")
    }
    config
  }

  private def printUsageAndExit(): Unit = {
    // scalastyle:off println
    println(
      """Usage:
        |  DotFieldLineageDemo [--dot <sqlflow.dot>] [--field <fieldName>]
        |                     [--node <nodeNameOrId>] [--max-paths <n>]
        |
        |Options:
        |  --dot <path>       Dot file path, default: target/sqlflow-debug/sqlflow.dot
        |  --field <field>    Target field, default: gold_zz_net_profit
        |  --node <node>      Optional target node filter by display name or unique id
        |  --max-paths <n>    Max field paths to print, default: 50
        |
        |Examples:
        |  DotFieldLineageDemo
        |  DotFieldLineageDemo --field gold_zz_net_profit
        |  DotFieldLineageDemo --field gold_zz_net_profit --node Project_3e099f3
        |  DotFieldLineageDemo --dot target/sqlflow-debug/sqlflow.dot
        |""".stripMargin)
    // scalastyle:on println
    sys.exit(0)
  }

  private def parseDot(dotPath: String): ParsedDot = {
    val path = Paths.get(dotPath)
    if (!Files.exists(path)) {
      throw new IllegalArgumentException(s"Dot file not found: ${path.toAbsolutePath}")
    }

    val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toVector
    val nodes = mutable.LinkedHashMap[String, DotNode]()
    val edges = mutable.ArrayBuffer[DotEdge]()

    var index = 0
    while (index < lines.length) {
      val line = lines(index)
      line match {
        case NodeStartPattern(nodeId) =>
          val block = mutable.ArrayBuffer[String](line)
          index += 1
          while (index < lines.length && !lines(index).contains("</table>>];")) {
            block += lines(index)
            index += 1
          }
          if (index < lines.length) {
            block += lines(index)
          }
          val parsedNode = parseNodeBlock(nodeId, block.toSeq)
          nodes += parsedNode.uniqueId -> parsedNode

        case EdgePattern(srcId, srcPort, dstId, dstPort) =>
          edges += DotEdge(srcId, srcPort, dstId, dstPort)

        case _ =>
      }
      index += 1
    }

    buildParsedDot(nodes.toMap, edges.toSeq)
  }

  private def parseNodeBlock(nodeId: String, block: Seq[String]): DotNode = {
    val displayName = block.collectFirst {
      case NodeNamePattern(rawName) => decodeHtml(stripTags(rawName))
    }.getOrElse(nodeId)

    val fieldsByPort = block.flatMap {
      case FieldPattern(port, rawFieldName) =>
        Some(port.toInt -> decodeHtml(stripTags(rawFieldName)))
      case _ =>
        None
    }.toMap

    DotNode(nodeId, displayName, fieldsByPort)
  }

  private def buildParsedDot(nodes: Map[String, DotNode], edges: Seq[DotEdge]): ParsedDot = {
    val incoming = mutable.HashMap[FieldRef, mutable.ArrayBuffer[FieldRef]]()
    val outgoing = mutable.HashMap[FieldRef, mutable.ArrayBuffer[FieldRef]]()

    edges.foreach { edge =>
      toFieldRef(edge.srcId, edge.srcPort, nodes).foreach { src =>
        toFieldRef(edge.dstId, edge.dstPort, nodes).foreach { dst =>
          incoming.getOrElseUpdate(dst, mutable.ArrayBuffer.empty) += src
          outgoing.getOrElseUpdate(src, mutable.ArrayBuffer.empty) += dst
        }
      }
    }

    ParsedDot(
      nodes = nodes,
      edges = edges,
      incomingFieldEdges = incoming.mapValues(_.distinct.toSeq).toMap,
      outgoingFieldEdges = outgoing.mapValues(_.distinct.toSeq).toMap)
  }

  private def toFieldRef(
      nodeId: String,
      port: String,
      nodes: Map[String, DotNode]): Option[FieldRef] = {
    if (port == "nodeName") {
      None
    } else {
      val portIndex = port.toInt
      nodes.get(nodeId).flatMap { node =>
        if (node.fieldsByPort.contains(portIndex)) {
          Some(FieldRef(nodeId, portIndex))
        } else {
          None
        }
      }
    }
  }

  private def resolveTargets(parsedDot: ParsedDot, config: CliConfig): Seq[FieldEndpoint] = {
    val matchedFields = parsedDot.nodes.values.toSeq.flatMap { node =>
      node.fieldsByPort.collect {
        case (port, fieldName) if fieldName == config.fieldName =>
          FieldEndpoint(node.uniqueId, node.displayName, port, fieldName)
      }
    }.sortBy(endpoint => (endpoint.nodeName, endpoint.nodeId, endpoint.port))

    val filteredTargets = config.nodeFilter match {
      case Some(filter) =>
        matchedFields.filter { endpoint =>
          endpoint.nodeName == filter || endpoint.nodeId == filter
        }
      case None =>
        matchedFields
    }

    if (filteredTargets.isEmpty) {
      val filterDesc = config.nodeFilter.map(node => s", node=$node").getOrElse("")
      throw new IllegalArgumentException(
        s"No target field found for ${config.fieldName}$filterDesc")
    }

    filteredTargets
  }

  private def collectImmediateUpstreams(
      parsedDot: ParsedDot,
      target: FieldEndpoint): Seq[FieldEndpoint] = {
    val fieldRef = FieldRef(target.nodeId, target.port)
    parsedDot.incomingFieldEdges.getOrElse(fieldRef, Nil)
      .distinct
      .map(toFieldEndpoint(_, parsedDot.nodes))
      .sortBy(endpoint => (endpoint.nodeName, endpoint.nodeId, endpoint.port))
  }

  private def collectFieldPaths(
      parsedDot: ParsedDot,
      target: FieldEndpoint,
      maxPaths: Int): Seq[FieldPath] = {
    val results = mutable.ArrayBuffer[Seq[FieldRef]]()
    val targetRef = FieldRef(target.nodeId, target.port)

    def traverse(current: FieldRef, path: List[FieldRef], visited: Set[FieldRef]): Unit = {
      if (results.size >= maxPaths) {
        return
      }

      val parents = parsedDot.incomingFieldEdges.getOrElse(current, Nil)
        .filterNot(visited.contains)
        .distinct
        .sortBy(ref => (parsedDot.nodes(ref.nodeId).displayName, ref.nodeId, ref.port))

      if (parents.isEmpty) {
        results += path
      } else {
        parents.foreach { parent =>
          traverse(parent, parent :: path, visited + parent)
        }
      }
    }

    traverse(targetRef, List(targetRef), Set(targetRef))
    results.toSeq.map { path =>
      FieldPath(path.map(toFieldEndpoint(_, parsedDot.nodes)))
    }
  }

  private def toFieldEndpoint(
      ref: FieldRef,
      nodes: Map[String, DotNode]): FieldEndpoint = {
    val node = nodes(ref.nodeId)
    FieldEndpoint(ref.nodeId, node.displayName, ref.port, node.fieldsByPort(ref.port))
  }

  private def stripTags(raw: String): String = {
    raw.replaceAll("<[^>]+>", "")
  }

  private def decodeHtml(raw: String): String = {
    raw.replaceAll("&lt;", "<")
      .replaceAll("&gt;", ">")
      .replaceAll("&amp;", "&")
  }

  private def printMatchedTargets(config: CliConfig, targets: Seq[FieldEndpoint]): Unit = {
    // scalastyle:off println
    println("=== Dot Input ===")
    println(s"path: ${Paths.get(config.dotPath).toAbsolutePath}")
    println(s"field: ${config.fieldName}")
    config.nodeFilter.foreach(node => println(s"node filter: $node"))
    println()

    println("=== Matched Target Fields ===")
    println(s"matched target count: ${targets.size}")
    targets.zipWithIndex.foreach { case (target, index) =>
      println(s"${index + 1}. ${formatEndpoint(target)}")
    }
    println()
    // scalastyle:on println
  }

  private def printTargetSummary(
      target: FieldEndpoint,
      immediateUpstreams: Seq[FieldEndpoint],
      fieldPaths: Seq[FieldPath],
      index: Int,
      totalTargets: Int,
      config: CliConfig): Unit = {
    val uniqueSourceFields = fieldPaths.flatMap(_.nodes.headOption).distinct

    // scalastyle:off println
    println(s"=== Field Summary ${index + 1}/$totalTargets ===")
    println(s"target: ${formatEndpoint(target)}")
    println(s"immediate upstream field count: ${immediateUpstreams.size}")
    println(s"source field count: ${uniqueSourceFields.size}")
    println(s"printed path count: ${fieldPaths.size}")
    if (fieldPaths.size >= config.maxPaths) {
      println(s"note: path output truncated at --max-paths=${config.maxPaths}")
    }
    println()

    println("=== Immediate Upstream Fields ===")
    printEndpoints(immediateUpstreams)
    println()

    println("=== Source Fields ===")
    printEndpoints(uniqueSourceFields.sortBy(endpoint => (endpoint.nodeName, endpoint.port)))
    println()

    println("=== Full Field Paths ===")
    printFieldPaths(fieldPaths)
    println()
    // scalastyle:on println
  }

  private def formatEndpoint(endpoint: FieldEndpoint): String = {
    s"${endpoint.nodeName}(${endpoint.nodeId}).${endpoint.fieldName}[port=${endpoint.port}]"
  }

  private def printEndpoints(endpoints: Seq[FieldEndpoint]): Unit = {
    // scalastyle:off println
    if (endpoints.isEmpty) {
      println("(none)")
    } else {
      endpoints.foreach { endpoint =>
        println(s"- ${formatEndpoint(endpoint)}")
      }
    }
    // scalastyle:on println
  }

  private def printFieldPaths(paths: Seq[FieldPath]): Unit = {
    // scalastyle:off println
    if (paths.isEmpty) {
      println("(none)")
    } else {
      paths.zipWithIndex.foreach { case (path, index) =>
        println(s"path ${index + 1} (${path.hopCount} hops)")
        println(s"  ${path.nodes.map(formatEndpoint).mkString(" -> ")}")
      }
    }
    // scalastyle:on println
  }
}
