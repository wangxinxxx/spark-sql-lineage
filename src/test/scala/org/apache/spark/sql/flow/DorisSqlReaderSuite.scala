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

import org.scalatest.funsuite.AnyFunSuite

class DorisSqlReaderSuite extends AnyFunSuite {
  test("extract sql from details json") {
    val details = """{"name":"demo","sql":"select * from temp.demo"}"""

    assert(DorisSqlReader.extractSql(details).contains("select * from temp.demo"))
  }

  test("build doris query with optional filters") {
    val config = DorisSqlReaderConfig(
      whereClause = Some("dt = '2026-04-14'"),
      orderBy = Some("job_name ASC"),
      limit = Some(10))

    assert(DorisSqlReader.query(config) ==
      "SELECT job_name, details FROM temp.dim_dg_58dp_job_info_full_1d " +
        "WHERE dt = '2026-04-14' ORDER BY job_name ASC LIMIT 10")
  }

  test("sanitize job name as sql file name") {
    assert(DorisSqlFileExporter.sanitizedFileName("foo/bar job").contains("foo_bar_job"))
  }

  test("exporter default query contains doris filters") {
    val query = DorisSqlReader.query(DorisSqlFileExporter.defaultConfig)

    assert(query.contains("AND coalesce(sql, '') <> ''"))
    assert(query.contains("AND job_status_id = 1"))
    assert(query.contains("AND job_type_id IN (3, 16)"))
    assert(query.contains("ORDER BY job_name ASC"))
    assert(query.contains("LIMIT 10000"))
  }
}
