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

import java.io.File

import org.apache.spark.sql.flow.sink.{FieldLineageJsonFileFormat, Neo4jAuraFieldLineageSink}

object LineageFileToNeo4j {
//  private val DefaultNeo4jUri = "neo4j://127.0.0.1:7687"
  private val DefaultNeo4jUri = "neo4j://10.40.16.226:7687"
  private val DefaultNeo4jUser = "neo4j"
  private val DefaultNeo4jPassword = "wx123456.."
  private val DefaultBatchSize = 500
  private val DefaultInputPath = new File("output/sqlflow-debug/parallel-run-0604-2")
  // Edit one of these two variables to resume a large import without changing CLI args.
  private val StartFromPathContains = "04784"
  private val StartAfterPathContains = ""

  def main(args: Array[String]): Unit = {
    val input = args.headOption.map(new File(_)).getOrElse(defaultInputDir())
    val uri = args.lift(1).orElse(sys.env.get("NEO4J_URI")).getOrElse(DefaultNeo4jUri)
    val user = args.lift(2).orElse(sys.env.get("NEO4J_USER")).getOrElse(DefaultNeo4jUser)
    val password = args.lift(3).orElse(sys.env.get("NEO4J_PASSWORD")).getOrElse(DefaultNeo4jPassword)
    val batchSize = args.lift(4).orElse(sys.env.get("NEO4J_BATCH_SIZE"))
      .map(_.toInt)
      .getOrElse(DefaultBatchSize)
    if (batchSize <= 0) {
      throw new IllegalArgumentException(s"Batch size must be positive, but got: $batchSize")
    }
    val allLineageFiles = FieldLineageJsonFileFormat.lineageFiles(input)
    if (allLineageFiles.isEmpty) {
      throw new IllegalArgumentException(
        s"No lineage jsonl files found under: ${input.getAbsolutePath}")
    }
    val lineageFiles = selectLineageFiles(allLineageFiles)

    val sink = Neo4jAuraFieldLineageSink(uri, user, password)
    var recordCount = 0
    var rawNodeCount = 0
    var rawEdgeCount = 0

    sink.withReusableDriver { driver =>
      lineageFiles.foreach { file =>
        val stats = sink.appendRecordFile(file, batchSize, driver)
        recordCount += stats.recordCount
        rawNodeCount += stats.rawNodeCount
        rawEdgeCount += stats.rawEdgeCount
        // scalastyle:off println
        println(s"[LINEAGE-IMPORT] file=${file.getAbsolutePath}, records=${stats.recordCount}")
        // scalastyle:on println
      }
    }

    // scalastyle:off println
    println(
      s"=== Lineage import done: files=${lineageFiles.size}, records=$recordCount, " +
        s"rawNodes=$rawNodeCount, rawEdges=$rawEdgeCount ===")
    // scalastyle:on println
  }

  private def selectLineageFiles(lineageFiles: Seq[File]): Seq[File] = {
    if (lineageFiles.size <= 1) {
      return lineageFiles
    }

    val startFrom = StartFromPathContains.trim
    val startAfter = StartAfterPathContains.trim
    if (startFrom.nonEmpty && startAfter.nonEmpty) {
      throw new IllegalArgumentException(
        "Only one of StartFromPathContains or StartAfterPathContains can be set")
    }

    val selected =
      if (startFrom.nonEmpty) {
        selectByPathToken(lineageFiles, startFrom, inclusive = true)
      } else if (startAfter.nonEmpty) {
        selectByPathToken(lineageFiles, startAfter, inclusive = false)
      } else {
        lineageFiles
      }

    if (selected.isEmpty) {
      throw new IllegalArgumentException("No lineage jsonl files selected after applying resume filter")
    }

    // scalastyle:off println
    println(
      s"=== Lineage import plan: selected=${selected.size}/${lineageFiles.size}, " +
        s"first=${selected.head.getAbsolutePath} ===")
    // scalastyle:on println
    selected
  }

  private def selectByPathToken(
      lineageFiles: Seq[File],
      pathToken: String,
      inclusive: Boolean): Seq[File] = {
    val matchedIndexes = lineageFiles.zipWithIndex.collect {
      case (file, idx) if file.getAbsolutePath.contains(pathToken) => idx
    }
    if (matchedIndexes.isEmpty) {
      throw new IllegalArgumentException(
        s"No lineage jsonl file matched resume token: $pathToken")
    }
    if (matchedIndexes.size > 1) {
      val matchedPaths = matchedIndexes.map(lineageFiles(_).getAbsolutePath).mkString(", ")
      throw new IllegalArgumentException(
        s"Resume token matched multiple lineage files: $matchedPaths")
    }
    val startIndex = if (inclusive) matchedIndexes.head else matchedIndexes.head + 1
    lineageFiles.drop(startIndex)
  }

  private def defaultInputDir(): File = {
    if (DefaultInputPath.isFile && DefaultInputPath.getName.endsWith(".jsonl")) {
      return DefaultInputPath
    }

    if (DefaultInputPath.isDirectory) {
      val directJsonlFiles = FieldLineageJsonFileFormat.lineageFiles(DefaultInputPath)
      if (directJsonlFiles.nonEmpty) {
        return DefaultInputPath
      }

      val candidates = Option(DefaultInputPath.listFiles()).getOrElse(Array.empty)
        .filter(file => file.isDirectory && file.getName.startsWith("parallel-"))
        .sortBy(_.getName)
      val latest = candidates.lastOption.getOrElse {
        throw new IllegalArgumentException(
          s"No lineage jsonl files or parallel output directory found under: ${DefaultInputPath.getAbsolutePath}")
      }
      val reportsDir = new File(latest, "reports")
      if (!reportsDir.isDirectory) {
        throw new IllegalArgumentException(
          s"Default reports directory does not exist: ${reportsDir.getAbsolutePath}")
      }
      return reportsDir
    }

    throw new IllegalArgumentException(
      s"Default input path does not exist: ${DefaultInputPath.getAbsolutePath}")
  }
}
