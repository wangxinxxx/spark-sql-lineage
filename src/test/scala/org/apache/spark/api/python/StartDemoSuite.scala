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
import java.nio.file.Files

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

class StartDemoSuite extends AnyFunSuite {
  test("localize add jar path under local jars root") {
    val sqlText =
      "add jar /home/hdp_58dp/udf/MysqlOutput-2.0.jar;" +
        "add jar 'hdfs://nameservice1/home/hdp_58dp/udf/Other.jar';"

    val actual = StartDemo.localizeAddJarPaths(sqlText)

    assert(actual.contains(
      "add jar /Users/zz/work/jars/home/hdp_58dp/udf/MysqlOutput-2.0.jar"))
    assert(actual.contains(
      "add jar '/Users/zz/work/jars/home/hdp_58dp/udf/Other.jar'"))
  }

  test("strip comments before splitting sql statements") {
    val sqlText =
      """-- header;
        |--set hive.exec.parallel=true;
        |/*
        |drop table commented_out;
        |*/
        |select ';' as value;
        |-- tail;""".stripMargin

    val statements = StartDemo.splitStatements(StartDemo.prepareSqlText(sqlText))

    assert(statements == Seq("select ';' as value"))
  }

  test("filter alter table drop partition statements") {
    assert(StartDemo.isFilteredStatement(
      "alter table t drop if exists partition (dt <= '2026-04-06')"))
    assert(StartDemo.isFilteredStatement("ALTER TABLE t DROP PARTITION(dt < '2026-04-12')"))
    assert(!StartDemo.isFilteredStatement("alter table t add partition(dt = '2026-04-13')"))
  }

  test("filter drop table statements") {
    assert(StartDemo.isFilteredStatement("drop table if exists tmp_table"))
    assert(StartDemo.isFilteredStatement("DROP TABLE tmp_table"))
    assert(!StartDemo.isFilteredStatement("create table tmp_table as select 1"))
  }

  test("filter truncate table statements") {
    assert(StartDemo.isFilteredStatement(
      "TRUNCATE TABLE hdp_zhuanzhuan_ads_lux.t PARTITION (p20260414)"))
    assert(StartDemo.isFilteredStatement("truncate table db.table_name"))
  }

  test("filter delete from statements") {
    assert(StartDemo.isFilteredStatement("DELETE FROM db.table_name WHERE dt = '2026-04-14'"))
    assert(StartDemo.isFilteredStatement("delete from table_name"))
  }

  test("extract temporary function name from create function statement") {
    assert(StartDemo.extractTempFunctionName(
      "create temporary function dboutput as 'org.example.DbOutput'").contains("dboutput"))
    assert(StartDemo.extractTempFunctionName(
      "CREATE TEMP FUNCTION `JsonUDF` AS 'org.example.JsonUdf'").contains("jsonudf"))
    assert(StartDemo.extractTempFunctionName("add jar /tmp/test.jar").isEmpty)
  }

  test("extract add jar path and file name from statement") {
    val statement =
      "add jar 'viewfs://58-cluster/home/hdp_58dp/udf/mysql-connector-java-5.1.6.jar'"

    assert(StartDemo.extractAddJarPaths(statement) ==
      Seq("viewfs://58-cluster/home/hdp_58dp/udf/mysql-connector-java-5.1.6.jar"))
    assert(StartDemo.extractAddJarFileNames(statement) == Seq("mysql-connector-java-5.1.6.jar"))
  }

  test("append timestamp suffix before extension") {
    assert(
      StartDemo.withTimestampSuffix("batch-script-summary.tsv", "20260416-154500") ==
        "batch-script-summary-20260416-154500.tsv")
    assert(
      StartDemo.withTimestampSuffix("batch-script-summary", "20260416-154500") ==
        "batch-script-summary-20260416-154500")
  }

  test("skip duplicate add jar registrations by jar file name") {
    val firstStatement =
      "add jar /Users/zz/work/jars/home/hdp_58dp/udf/mysql-connector-java-5.1.6.jar"
    val secondStatement =
      "add jar /Users/zz/work/jars/home/hdp_ubu_zhuanzhuan/resultdata/wangyu27/conf/" +
        "mysql-connector-java-5.1.6.jar"
    val registeredJarPaths = mutable.Set.empty[String]

    assert(!StartDemo.isDuplicateAddJarRegistration(firstStatement, registeredJarPaths))

    StartDemo.rememberAddJarRegistration(firstStatement, registeredJarPaths)

    assert(!StartDemo.isDuplicateAddJarRegistration(secondStatement, registeredJarPaths))
  }

  test("skip duplicate add jar registrations only for the same path") {
    val statement =
      "add jar /Users/zz/work/jars/home/hdp_58dp/udf/mysql-connector-java-5.1.6.jar"
    val registeredJarPaths = mutable.Set.empty[String]

    assert(!StartDemo.isDuplicateAddJarRegistration(statement, registeredJarPaths))

    StartDemo.rememberAddJarRegistration(statement, registeredJarPaths)

    assert(StartDemo.isDuplicateAddJarRegistration(statement, registeredJarPaths))
  }

  test("skip duplicate temporary function registrations") {
    val statement =
      "create temporary function start_demo_ascii as " +
        "'org.apache.hadoop.hive.ql.udf.UDFAscii'"
    val registeredTempFunctions = mutable.Map.empty[String, String]

    assert(!StartDemo.isDuplicateTempFunctionRegistration(statement, registeredTempFunctions))

    StartDemo.rememberTempFunctionRegistration(statement, registeredTempFunctions)

    assert(StartDemo.isDuplicateTempFunctionRegistration(statement, registeredTempFunctions))
  }

  test("do not skip temporary function registrations with different definitions") {
    val firstStatement =
      "create temporary function start_demo_ascii as " +
        "'org.apache.hadoop.hive.ql.udf.UDFAscii'"
    val secondStatement =
      "create temporary function start_demo_ascii as " +
        "'org.apache.hadoop.hive.ql.udf.UDFHex'"
    val registeredTempFunctions = mutable.Map.empty[String, String]

    StartDemo.rememberTempFunctionRegistration(firstStatement, registeredTempFunctions)

    assert(!StartDemo.isDuplicateTempFunctionRegistration(
      secondStatement,
      registeredTempFunctions))
  }

  test("classify with add jar directory from parent name or active setup statements") {
    val baseDir = Files.createTempDirectory("start-demo-suite-")
    val plainFile = baseDir.resolve("sqls").resolve("plain.sql").toFile
    val parentMarkedFile = baseDir.resolve("sqls_with_add_jar").resolve("marked.sql").toFile

    assert(StartDemo.requiresWithAddJarDirectory(parentMarkedFile, "select 1"))
    assert(StartDemo.requiresWithAddJarDirectory(
      plainFile,
      "create temporary function demo_udf as 'pkg.DemoUdf';\nselect demo_udf(1);"))
    assert(!StartDemo.requiresWithAddJarDirectory(
      plainFile,
      "-- add jar viewfs://cluster/demo.jar;\nselect 'add jar';"))
  }

  test("move sql files to sibling success and failed directories with add jar split") {
    val baseDir = Files.createTempDirectory("start-demo-archive-suite-")
    val plainDir = Files.createDirectories(baseDir.resolve("sqls"))
    val withAddJarDir = Files.createDirectories(baseDir.resolve("sqls_with_add_jar"))
    val plainFile = plainDir.resolve("plain.sql")
    val withAddJarFile = withAddJarDir.resolve("with_add_jar.sql")

    Files.write(plainFile, "select 1;".getBytes(StandardCharsets.UTF_8))
    Files.write(
      withAddJarFile,
      "add jar viewfs://cluster/demo.jar;".getBytes(StandardCharsets.UTF_8))

    val successTarget = StartDemo.moveToArchive(plainFile.toFile, requiresWithAddJarDir = false, "SUCCESS")
    val failedTarget = StartDemo.moveToArchive(
      withAddJarFile.toFile,
      requiresWithAddJarDir = true,
      "FAILURE")

    assert(successTarget.toPath == baseDir.resolve("success").resolve("plain.sql"))
    assert(Files.exists(successTarget.toPath))
    assert(!Files.exists(plainFile))

    assert(failedTarget.toPath == baseDir.resolve("failed_with_add_jar").resolve("with_add_jar.sql"))
    assert(Files.exists(failedTarget.toPath))
    assert(!Files.exists(withAddJarFile))
  }
}
