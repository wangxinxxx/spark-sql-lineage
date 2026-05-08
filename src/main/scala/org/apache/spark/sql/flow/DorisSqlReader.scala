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

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper

case class DorisSqlReaderConfig(
    jdbcUrl: String = "jdbc:mysql://10.40.87.153:9030/",
    table: String = "temp.dim_dg_58dp_job_info_full_1d",
    user: String = "test_user",
    password: String = "555a3EE8rI0",
    fetchSize: Int = 1000,
    whereClause: Option[String] = None,
    orderBy: Option[String] = None,
    limit: Option[Int] = None)

case class DorisSqlRow(rowNumber: Int, jobName: String, sql: String, details: String)

class DorisSqlReader(config: DorisSqlReaderConfig = DorisSqlReaderConfig()) {
  def readRows(): Seq[DorisSqlRow] = {
    DorisSqlReader.withConnection(config) { connection =>
      val statement = connection.prepareStatement(DorisSqlReader.query(config))
      statement.setFetchSize(config.fetchSize)
      val resultSet = statement.executeQuery()
      val rows = mutable.ArrayBuffer.empty[DorisSqlRow]
      try {
        var rowNumber = 1
        while (resultSet.next()) {
          DorisSqlReader.row(resultSet, rowNumber).foreach(rows += _)
          rowNumber += 1
        }
        rows.toSeq
      } finally {
        resultSet.close()
        statement.close()
      }
    }
  }

  def readSqlRows(): Seq[XlsxSqlRow] = {
    readRows().map { row =>
      XlsxSqlRow(row.rowNumber, row.jobName, "", row.sql)
    }
  }
}

object DorisSqlReader {
  private val JsonMapper = new ObjectMapper()

  def readRows(config: DorisSqlReaderConfig = DorisSqlReaderConfig()): Seq[DorisSqlRow] = {
    new DorisSqlReader(config).readRows()
  }

  def readSqlRows(config: DorisSqlReaderConfig = DorisSqlReaderConfig()): Seq[XlsxSqlRow] = {
    new DorisSqlReader(config).readSqlRows()
  }

  private[flow] def extractSql(details: String): Option[String] = {
    if (details == null || details.trim.isEmpty) {
      None
    } else {
      val root = JsonMapper.readTree(details)
      Option(root.path("sql"))
        .filterNot(node => node.isMissingNode || node.isNull)
        .map(_.asText().trim)
        .filter(_.nonEmpty)
    }
  }

  private[flow] def query(config: DorisSqlReaderConfig): String = {
    val whereText = config.whereClause.map(_.trim).filter(_.nonEmpty).map { clause =>
      if (clause.toLowerCase(Locale.ROOT).startsWith("where ")) {
        s" $clause"
      } else {
        s" WHERE $clause"
      }
    }.getOrElse("")
    val orderByText = config.orderBy.map(_.trim).filter(_.nonEmpty).map { clause =>
      if (clause.toLowerCase(Locale.ROOT).startsWith("order by ")) {
        s" $clause"
      } else {
        s" ORDER BY $clause"
      }
    }.getOrElse("")
    val limitText = config.limit.filter(_ > 0).map(limit => s" LIMIT $limit").getOrElse("")
    s"SELECT job_name, details FROM ${config.table}$whereText$orderByText$limitText"
  }

  private def row(resultSet: ResultSet, rowNumber: Int): Option[DorisSqlRow] = {
    val jobName = Option(resultSet.getString("job_name")).getOrElse("").trim
    val details = resultSet.getString("details")
    extractSql(details).map(sql => DorisSqlRow(rowNumber, jobName, sql, details))
  }

  private def withConnection[T](config: DorisSqlReaderConfig)(f: Connection => T): T = {
    loadDriver()
    val connection = DriverManager.getConnection(config.jdbcUrl, config.user, config.password)
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  private def loadDriver(): Unit = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
    } catch {
      case NonFatal(firstError) =>
        try {
          Class.forName("com.mysql.jdbc.Driver")
        } catch {
          case NonFatal(_) =>
            throw firstError
        }
    }
  }
}
