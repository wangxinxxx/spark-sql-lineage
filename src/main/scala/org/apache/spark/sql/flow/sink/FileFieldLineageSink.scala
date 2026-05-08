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

import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.{Codec, Source}

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.flow.{GraphNodeType, SQLFlowGraphEdge, SQLFlowGraphNode}

case class FieldLineageRecord(
    nodes: Seq[SQLFlowGraphNode],
    edges: Seq[SQLFlowGraphEdge],
    options: Map[String, String])

case class FileFieldLineageSink(outputDir: File) extends FieldLineageSink {
  private val outputFile = new File(outputDir, FileFieldLineageSink.DefaultFileName)

  override def toString: String = {
    s"${this.getClass.getSimpleName}(outputFile=${outputFile.getAbsolutePath})"
  }

  override def plannedWriteStats(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Neo4jFieldLineageWriteStats = {
    Neo4jFieldLineageWriteStats(nodes.size, edges.size)
  }

  override def append(
      nodes: Seq[SQLFlowGraphNode],
      edges: Seq[SQLFlowGraphEdge],
      options: Map[String, String]): Unit = synchronized {
    Option(outputFile.getParentFile).foreach(_.mkdirs())
    val writer = new PrintWriter(new OutputStreamWriter(
      new FileOutputStream(outputFile, true),
      StandardCharsets.UTF_8))
    try {
      writer.println(FieldLineageJsonFileFormat.toJsonLine(FieldLineageRecord(
        nodes,
        edges,
        options)))
    } finally {
      writer.close()
    }
  }
}

object FileFieldLineageSink {
  val DefaultFileName = "lineage-graphs.jsonl"
}

object FieldLineageJsonFileFormat {
  private val JsonMapper = new ObjectMapper()
  private val Version = Int.box(1)

  def toJsonLine(record: FieldLineageRecord): String = {
    JsonMapper.writeValueAsString(recordToMap(record))
  }

  def readRecords(input: File): Seq[FieldLineageRecord] = {
    lineageFiles(input).flatMap(readRecordFile)
  }

  def withRecordIterator[T](file: File)(f: Iterator[FieldLineageRecord] => T): T = {
    val source = Source.fromFile(file)(Codec.UTF8)
    try {
      val records = source.getLines().filter(_.trim.nonEmpty).map(fromJsonLine)
      f(records)
    } finally {
      source.close()
    }
  }

  def lineageFiles(input: File): Seq[File] = {
    if (input.isFile && input.getName.endsWith(".jsonl")) {
      Seq(input)
    } else if (input.isDirectory) {
      Option(input.listFiles()).getOrElse(Array.empty).flatMap(lineageFiles).sortBy(_.getAbsolutePath)
    } else {
      Nil
    }
  }

  private def readRecordFile(file: File): Seq[FieldLineageRecord] = {
    val source = Source.fromFile(file)(Codec.UTF8)
    try {
      source.getLines().filter(_.trim.nonEmpty).map(fromJsonLine).toVector
    } finally {
      source.close()
    }
  }

  private def fromJsonLine(line: String): FieldLineageRecord = {
    val recordMap = JsonMapper.readValue(line, classOf[JMap[String, Object]])
    val nodes = asMapSeq(recordMap.get("nodes")).map(nodeFromMap)
    val edges = asMapSeq(recordMap.get("edges")).map(edgeFromMap)
    val options = asStringMap(recordMap.get("options"))
    FieldLineageRecord(nodes, edges, options)
  }

  private def recordToMap(record: FieldLineageRecord): JMap[String, Object] = {
    Map[String, Object](
      "version" -> Version,
      "options" -> toObjectMap(record.options),
      "nodes" -> record.nodes.map(nodeToMap).asJava,
      "edges" -> record.edges.map(edgeToMap).asJava).asJava
  }

  private def nodeToMap(node: SQLFlowGraphNode): JMap[String, Object] = {
    Map[String, Object](
      "uniqueId" -> node.uniqueId,
      "ident" -> node.ident,
      "attributeNames" -> node.attributeNames.asJava,
      "schemaDDL" -> node.schemaDDL,
      "tpe" -> node.tpe.toString,
      "isCached" -> Boolean.box(node.isCached),
      "props" -> toObjectMap(node.props.toMap)).asJava
  }

  private def edgeToMap(edge: SQLFlowGraphEdge): JMap[String, Object] = {
    Map[String, Object](
      "fromId" -> edge.fromId,
      "fromIdx" -> edge.fromIdx.map(Int.box).orNull,
      "toId" -> edge.toId,
      "toIdx" -> edge.toIdx.map(Int.box).orNull,
      "props" -> toObjectMap(edge.props.toMap)).asJava
  }

  private def nodeFromMap(values: JMap[String, Object]): SQLFlowGraphNode = {
    SQLFlowGraphNode(
      asString(values.get("uniqueId")),
      asString(values.get("ident")),
      asStringSeq(values.get("attributeNames")),
      asString(values.get("schemaDDL")),
      GraphNodeType.withName(asString(values.get("tpe"))),
      asBoolean(values.get("isCached")),
      mutable.Map(asStringMap(values.get("props")).toSeq: _*))
  }

  private def edgeFromMap(values: JMap[String, Object]): SQLFlowGraphEdge = {
    SQLFlowGraphEdge(
      asString(values.get("fromId")),
      asOptionalInt(values.get("fromIdx")),
      asString(values.get("toId")),
      asOptionalInt(values.get("toIdx")),
      mutable.Map(asStringMap(values.get("props")).toSeq: _*))
  }

  private def toObjectMap(values: Map[String, String]): JMap[String, Object] = {
    values.map { case (key, value) => key -> value.asInstanceOf[Object] }.asJava
  }

  private def asMap(value: Object): JMap[String, Object] = {
    value.asInstanceOf[JMap[String, Object]]
  }

  private def asMapSeq(value: Object): Seq[JMap[String, Object]] = {
    value.asInstanceOf[java.util.List[JMap[String, Object]]].asScala.toSeq
  }

  private def asStringMap(value: Object): Map[String, String] = {
    if (value == null) {
      Map.empty
    } else {
      asMap(value).asScala.map { case (key, mapValue) =>
        key -> Option(mapValue).map(_.toString).getOrElse("")
      }.toMap
    }
  }

  private def asStringSeq(value: Object): Seq[String] = {
    value.asInstanceOf[java.util.List[Object]].asScala.map(asString).toSeq
  }

  private def asString(value: Object): String = {
    Option(value).map(_.toString).getOrElse("")
  }

  private def asBoolean(value: Object): Boolean = {
    value match {
      case b: java.lang.Boolean => b.booleanValue()
      case other => Option(other).exists(_.toString.toBoolean)
    }
  }

  private def asOptionalInt(value: Object): Option[Int] = {
    Option(value).map {
      case n: java.lang.Number => n.intValue()
      case other => other.toString.toInt
    }
  }
}
