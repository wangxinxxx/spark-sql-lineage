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

import java.nio.file.Files

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.flow.{GraphNodeType, SQLFlowGraphEdge, SQLFlowGraphNode}

class FileFieldLineageSinkSuite extends AnyFunSuite {
  test("write and read field lineage jsonl records") {
    val outputDir = Files.createTempDirectory("field-lineage-sink-test-").toFile
    val sink = FileFieldLineageSink(outputDir)
    val node = SQLFlowGraphNode(
      "table-1",
      "db.table",
      Seq("id", "name"),
      "`id` BIGINT, `name` STRING",
      GraphNodeType.TableNode,
      isCached = false,
      mutable.Map("source" -> "unit-test"))
    val edge = SQLFlowGraphEdge(
      "table-1",
      Some(0),
      "table-1",
      Some(1),
      mutable.Map("expr" -> "cast(id as string)"))
    val options = Map(
      "graphMode" -> "direct_table_field",
      "sqlFileName" -> "测试脚本.sql")

    sink.append(Seq(node), Seq(edge), options)

    val records = FieldLineageJsonFileFormat.readRecords(outputDir)
    assert(records.size == 1)
    assert(records.head.nodes == Seq(node))
    assert(records.head.edges == Seq(edge))
    assert(records.head.options == options)
  }
}

