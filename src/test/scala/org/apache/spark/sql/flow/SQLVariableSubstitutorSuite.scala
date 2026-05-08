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

import java.time.{LocalDateTime, ZoneId}

import org.scalatest.funsuite.AnyFunSuite

class SQLVariableSubstitutorSuite extends AnyFunSuite {
  private val baseContext = SQLVariableContext(
    runDateTime = LocalDateTime.of(2015, 8, 24, 17, 30, 0),
    zoneId = ZoneId.of("Asia/Shanghai"),
    moduleName = Some("demo_module"))

  test("replace documented named variables") {
    val input =
      """dt='${outFileSuffix}'
        |yesterday=${yesterday}
        |ds=${dateSuffix}
        |today=${today}
        |hour=${dateHourSuffix}
        |prev=${dateBeforeOneHourSuffix}
        |temp='${tempCatalog}'
        |data='${dataFile}'""".stripMargin

    val actual = SQLVariableSubstitutor.replace(input, baseContext)

    assert(actual.contains("dt='2015-08-23'"))
    assert(actual.contains("yesterday='2015-08-23'"))
    assert(actual.contains("ds=20150823"))
    assert(actual.contains("today='2015-08-24'"))
    assert(actual.contains("hour=2015082417"))
    assert(actual.contains("prev=2015082416"))
    assert(actual.contains("temp='/tmp/dw_tmp_file/'"))
    assert(actual.contains("data='/tmp/dw_tmp_file/demo_module/data'"))
  }

  test("selected date drives business date variables") {
    val context = baseContext.copy(
      selectedDateTime = Some(LocalDateTime.of(2014, 4, 4, 10, 0, 0)))

    val actual = SQLVariableSubstitutor.replace(
      "${startDate}|${monthBegin}|${monthEnd}|${quarterBegin}|${quarterEnd}",
      context)

    assert(actual == "'2014-04-03'|'2014-04-01'|'2014-04-30'|2014-04-01|2014-06-30")
  }

  test("replace compatible formula variables") {
    val context = baseContext.copy(
      selectedDateTime = Some(LocalDateTime.of(2015, 5, 6, 17, 0, 0)))
    val previousDayQuarter =
      "${((#day#==1) && ((#month#-1)%3==0))?" +
        "(#month#-2)/3+1:(#month#-1)/3+1}"

    assert(SQLVariableSubstitutor.replace("${#year# + 1 + '#month#'}", context) == "201605")
    assert(SQLVariableSubstitutor.replace("${(#month#-1)/3+1}", context) == "2")
    assert(SQLVariableSubstitutor.replace(previousDayQuarter, context) == "2")
    assert(SQLVariableSubstitutor.replace(
      "${#date(1,2,3):yyyy-MM-dd HH:mm:ss#}",
      context) == "2016-07-09 17:00:00")
    assert(SQLVariableSubstitutor.replace("${#date(0,0,-day):yyyyMMdd#}", context) == "20150430")
    assert(SQLVariableSubstitutor.replace(
      "${#date(0,-(month-1)%3,0):yyyyMM01#}",
      context) == "20150401")
  }

  test("replace safe bash date variables") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date +%Y%m%d%H -d '-1 hour'}",
      baseContext)

    assert(actual == "2015082416")
  }

  test("replace nested variables in safe bash date expressions") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date +%Y-%m-%d -d '-1 hour ${todayDateTime}'}|" +
        "$bash{date -d '${outFileSuffix} -7 day' +%Y-%m-%d}",
      baseContext)

    assert(actual == "2015-08-24|2015-08-16")
  }

  test("replace bash date offsets before explicit datetime text") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date +%Y-%m-%d -d '-1 hour ${todayDateTime}'}",
      baseContext)

    assert(actual == "2015-08-24")
  }

  test("replace abbreviated bash date offset units") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date '+%Y-%m-%d %H:%M:%S' -d '-20 min ${todayDateTime}'}",
      baseContext)

    assert(actual == "2015-08-24 17:10:00")
  }

  test("replace bash date offsets with spaced signs") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date +%Y-%m-%d -d ' - 1 day '}",
      baseContext)

    assert(actual == "2015-08-23")
  }

  test("replace bash date offsets with signed units") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date +%Y-%m-%d -d '-1 +day'}",
      baseContext)

    assert(actual == "2015-08-23")
  }

  test("replace bash date offsets after compact date text") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date +%Y-%m-%d -d '${todaySuffix} -3 day'}",
      baseContext)

    assert(actual == "2015-08-21")
  }

  test("replace bash date offsets with trailing ago after explicit date text") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date -d \"${outFileSuffix} 1 days ago \" \"+%Y-%m-%d\"}",
      baseContext)

    assert(actual == "2015-08-22")
  }

  test("replace bash date offsets with article amounts and trailing ago") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date -d'${outFileSuffix} a day ago' +%Y-%m-%d}",
      baseContext)

    assert(actual == "2015-08-22")
  }

  test("replace malformed bash date text with explicit date and bare hour token") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date +%Y-%m-%d -d ' hour ${dateSuffix}'}",
      baseContext)

    assert(actual == "2015-08-23")
  }

  test("replace bash date offsets with attached -d argument") {
    val actual = SQLVariableSubstitutor.replace(
      "$bash{date -d'${outFileSuffix} 1 day ago' +%Y-%m-%d}",
      baseContext)

    assert(actual == "2015-08-22")
  }

  test("replace bash date rounded epoch expressions") {
    val context = baseContext.copy(
      runDateTime = LocalDateTime.of(2015, 8, 24, 17, 34, 56))
    val actual = SQLVariableSubstitutor.replace(
      "'$bash{date -d @$(( $(date +%s) / 900 * 900 )) +'%Y-%m-%d %H:%M:00'}' AS proc_time",
      context)

    assert(actual == "'2015-08-24 17:30:00' AS proc_time")
  }

  test("replace bash rounded epoch expressions with second offsets") {
    val context = baseContext.copy(
      runDateTime = LocalDateTime.of(2015, 8, 24, 17, 34, 56))
    val sql =
      """DELETE FROM demo
        |WHERE proc_time < '$bash{date -d "@$(( $(date +%s) / 900 * 900 - 900 ))" +'%Y-%m-%d %H:%M:00'}'
        |    AND proc_time != '$bash{date -d "@$(( $(date -d "$(date +%Y-%m-%d) 00:00:00" +%s) - 900 ))" +'%Y-%m-%d %H:%M:00'}';
        |INSERT INTO demo
        |SELECT '$bash{date -d @$(( $(date +%s) / 900 * 900 )) +'%Y-%m-%d %H:%M:00'}' AS proc_time""".stripMargin

    val actual = SQLVariableSubstitutor.replace(sql, context)

    assert(actual.contains("proc_time < '2015-08-24 17:15:00'"))
    assert(actual.contains("proc_time != '2015-08-23 23:45:00'"))
    assert(actual.contains("SELECT '2015-08-24 17:30:00' AS proc_time"))
  }

  test("replace documented variables inside spark date functions") {
    val actual = SQLVariableSubstitutor.replace(
      "WHERE dt>=date_sub('${outFileSuffix}',1)",
      baseContext)

    assert(actual == "WHERE dt>=date_sub('2015-08-23',1)")
  }

  test("replace date format placeholders") {
    val actual = SQLVariableSubstitutor.replace(
      "if(d.spider_time is null, '${yyyy-MM-dd} 00:00:00', d.spider_time)",
      baseContext)

    assert(actual == "if(d.spider_time is null, '2015-08-23 00:00:00', d.spider_time)")
    assert(SQLVariableSubstitutor.replace("${yyyyMMdd}", baseContext) == "20150823")
  }

  test("preserve sql expressions that are not formulas") {
    val sql = """SELECT ${from_unixtime(tvh.voucher_date, "%Y-%m-%d")} stat_date"""

    assert(SQLVariableSubstitutor.replace(sql, baseContext) == sql)
  }

  test("replace variables from set statements") {
    val sql =
      """set financialBizLineList =
        |    '采购入库-采货侠厂商货源',
        |    '采购入库-门店配件耗材';
        |set deadLine = ${today};
        |select ${hiveconf:financialBizLineList} as lines, ${hivevar:deadLine} as dt;"""
        .stripMargin

    val actual = SQLVariableSubstitutor.replace(sql, baseContext)

    assert(actual.contains("'采购入库-采货侠厂商货源'"))
    assert(actual.contains("'采购入库-门店配件耗材' as lines"))
    assert(actual.contains("set deadLine = '2015-08-24';"))
    assert(actual.contains("'2015-08-24' as dt"))
  }

  test("replace hivevar variables from set statements") {
    val sql =
      """-- some upstream table comments
        |
        |set hivevar:c2b_home_order_source=40,41,49,69,57,58,2701027,2701019,2701020,2701033,2701016,42,43,55,56,61;
        |set hivevar:c2b_store_order_source=46,2706003;
        |set hivevar:c2b_mail_order_source=21,34,51,53,22,35,2701018,52,54,2705009,2701028,2701029,2701030,2701031;
        |select case when t.order_source_id in (${c2b_home_order_source}) then '上门回收'
        |            when t.order_source_id in (${c2b_store_order_source}) then '线下门店回收'
        |            when t.order_source_id in (${c2b_mail_order_source}) then '邮寄回收'
        |       end as order_source_type
        |from demo t;""".stripMargin

    val actual = SQLVariableSubstitutor.replace(sql, baseContext)

    assert(actual.contains("t.order_source_id in (40,41,49,69,57,58,2701027"))
    assert(actual.contains("t.order_source_id in (46,2706003)"))
    assert(actual.contains("t.order_source_id in (21,34,51,53,22,35,2701018"))
    assert(!actual.contains("${c2b_home_order_source}"))
  }

  test("replace at variables from set statements") {
    val sql =
      """SET @partition_d = '2026-04-14';
        |insert overwrite table demo PARTITION (p20260414)
        |select @partition_d as stat_date;""".stripMargin

    val actual = SQLVariableSubstitutor.replace(sql, baseContext)

    assert(actual.contains("SET @partition_d = '2026-04-14';"))
    assert(actual.contains("select '2026-04-14' as stat_date;"))
  }

  test("unknown simple variables are preserved") {
    assert(SQLVariableSubstitutor.replace("${start_date}", baseContext) == "${start_date}")
  }

  test("unsupported bash commands fail closed") {
    val error = intercept[IllegalArgumentException] {
      SQLVariableSubstitutor.replace("$bash{echo hello}", baseContext)
    }

    assert(error.getMessage.contains("Unsupported bash variable expression"))
  }
}
