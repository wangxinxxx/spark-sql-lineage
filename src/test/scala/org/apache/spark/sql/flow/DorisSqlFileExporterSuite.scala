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

package org.apache.spark.sql.flow

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

class DorisSqlFileExporterSuite extends AnyFunSuite {
  test("normalize exported sql comments spark set statements") {
    val sql =
      """Set spark.sql.inMemoryColumnarStorage.batchSize=1024 ;
        |SET spark.shuffle.service.enabled=true ;
        |select 1;""".stripMargin

    val actual = DorisSqlFileExporter.normalizeExportedSql(sql)

    assert(actual.contains("-- Set spark.sql.inMemoryColumnarStorage.batchSize=1024 ;"))
    assert(actual.contains("-- SET spark.shuffle.service.enabled=true ;"))
    assert(actual.contains("select 1;"))
  }

  test("normalize exported sql adds order by 1 to unordered row_number windows") {
    val sql =
      """select
        |  row_number() over(partition by user_id) as rn
        |from demo""".stripMargin

    val actual = DorisSqlFileExporter.normalizeExportedSql(sql)

    assert(actual.contains("row_number() over(partition by user_id ORDER BY 1)"))
  }

  test("normalize exported sql keeps ordered row_number windows unchanged") {
    val sql =
      """select
        |  row_number() over(partition by user_id order by ts desc) as rn
        |from demo""".stripMargin

    assert(DorisSqlFileExporter.normalizeExportedSql(sql) == sql)
  }

  test("requires with add jar directory for active add jar or temp function statements") {
    val addJarSql =
      """add jar viewfs://cluster/demo.jar;
        |select 1;""".stripMargin
    val tempFunctionSql =
      """create temp function demo_udf as 'pkg.DemoUdf';
        |select demo_udf(1);""".stripMargin

    assert(DorisSqlFileExporter.requiresWithAddJarDirectory(addJarSql))
    assert(DorisSqlFileExporter.requiresWithAddJarDirectory(tempFunctionSql))
  }

  test("requires with add jar directory ignores commented statements") {
    val sql =
      """/*
        |add jar viewfs://cluster/demo.jar;
        |create temporary function demo_udf as 'pkg.DemoUdf';
        |*/
        |-- add jar viewfs://cluster/another.jar;
        |-- create temp function demo_udf as 'pkg.DemoUdf';
        |select 'add jar';
        |""".stripMargin

    assert(!DorisSqlFileExporter.requiresWithAddJarDirectory(sql))
  }

  test("export rows writes each sql only once to the target directory") {
    val baseDir = Files.createTempDirectory("doris-sql-exporter-suite-")
    val outputDir = baseDir.resolve("sqls")
    val rawDir = baseDir.resolve("raw")
    val withAddJarDir = outputDir.resolveSibling("sqls_with_add_jar")
    val plainFile = outputDir.resolve("plain_job.sql")
    val jarFile = withAddJarDir.resolve("jar_job.sql")
    val tempFile = withAddJarDir.resolve("temp_job.sql")

    Files.createDirectories(outputDir)
    Files.createDirectories(withAddJarDir)
    Files.write(
      outputDir.resolve("jar_job.sql"),
      "stale".getBytes(StandardCharsets.UTF_8))
    Files.write(
      outputDir.resolve("temp_job.sql"),
      "stale".getBytes(StandardCharsets.UTF_8))

    val rows = Seq(
      DorisSqlRow(1, "plain job", "select 1;", ""),
      DorisSqlRow(2, "jar job", "add jar viewfs://cluster/demo.jar;\nselect 2;", ""),
      DorisSqlRow(
        3,
        "temp job",
        "create temporary function demo_udf as 'pkg.DemoUdf';\nselect demo_udf(3);",
        ""))

    val exported = DorisSqlFileExporter.exportRows(rows, outputDir, rawDir)

    assert(exported == Seq(plainFile, jarFile, tempFile))
    assert(Files.exists(plainFile))
    assert(!Files.exists(withAddJarDir.resolve("plain_job.sql")))
    assert(Files.exists(jarFile))
    assert(!Files.exists(outputDir.resolve("jar_job.sql")))
    assert(Files.exists(tempFile))
    assert(!Files.exists(outputDir.resolve("temp_job.sql")))
    assert(Files.exists(rawDir.resolve("plain_job.sql")))
    assert(Files.exists(rawDir.resolve("jar_job.sql")))
    assert(Files.exists(rawDir.resolve("temp_job.sql")))
  }

  test("export rows preserves comment-only variable examples") {
    val baseDir = Files.createTempDirectory("doris-sql-exporter-comments-suite-")
    val outputDir = baseDir.resolve("sqls")
    val rawDir = baseDir.resolve("raw")
    val sql =
      """-- example: ${#date(0,0,-N):yyyy-MM-dd#}
        |insert overwrite table demo partition(dt='${#date(0,0,-1):yyyy-MM-dd#}')
        |select 1;""".stripMargin

    val exported = DorisSqlFileExporter.exportRows(
      Seq(DorisSqlRow(7532, "comment example", sql, "")),
      outputDir,
      rawDir)

    val exportedSql = new String(Files.readAllBytes(exported.head), StandardCharsets.UTF_8)

    assert(exported.size == 1)
    assert(exportedSql.contains("-- example: ${#date(0,0,-N):yyyy-MM-dd#}"))
    assert(exportedSql.contains("partition(dt='2026-05-19')"))
  }
}
