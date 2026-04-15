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

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.util.control.NonFatal

object DorisSqlFileExporter {
  private val DefaultOutputDir = Paths.get("src/main/resources/sqls")
  private val DefaultRawOutputDir = Paths.get("src/main/raw")
  private[flow] val DefaultWhereClause =
    """dt = '2026-04-14'
      |AND dept_name = '转转大数据研发部'
      |AND coalesce(sql, '') <> ''
      |AND job_status_id = 1
      |and schedule_id = '536395'
      |AND job_type_id IN (2, 16)""".stripMargin

  private[flow] def defaultConfig: DorisSqlReaderConfig = {
    DorisSqlReaderConfig(whereClause = Some(DefaultWhereClause), limit = Some(10000))
  }

  def main(args: Array[String]): Unit = {
    val config = defaultConfig
    val outputDir = args.lift(1).map(Paths.get(_)).getOrElse(DefaultOutputDir)
    val files = export(config, outputDir)

    // scalastyle:off println
    println(s"Exported ${files.size} sql files to ${outputDir.toAbsolutePath}")
    // scalastyle:on println
  }

  def export(
      config: DorisSqlReaderConfig = defaultConfig,
      outputDir: Path = DefaultOutputDir): Seq[Path] = {
    Files.createDirectories(outputDir)
    Files.createDirectories(DefaultRawOutputDir)
    DorisSqlReader.readRows(config).map { row =>
      val file = outputDir.resolve(fileName(row))
      val rawFile = DefaultRawOutputDir.resolve(fileName(row))
      writeSqlFile(rawFile.toFile, row.sql)
      try {
        val replacedSql = SQLVariableSubstitutor.replace(row.sql)
        writeSqlFile(file.toFile, replacedSql)
        file
      } catch {
        case NonFatal(error) =>
          throw new IllegalArgumentException(
            s"Failed to export SQL file: row=${row.rowNumber}, " +
              s"job_name=${row.jobName}, file=${file.toAbsolutePath}, " +
              s"raw_file=${rawFile.toAbsolutePath}",
            error)
      }
    }
  }

  private def fileName(row: DorisSqlRow): String = {
    val name = sanitizedFileName(row.jobName).getOrElse(f"${row.rowNumber}%06d")
    s"$name.sql"
  }

  private[flow] def sanitizedFileName(value: String): Option[String] = {
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.replaceAll("""[\\/:*?"<>|\p{Cntrl}]+""", "_"))
      .map(_.replaceAll("\\s+", "_"))
      .map(_.replaceAll("_+", "_"))
      .map(_.stripPrefix("_").stripSuffix("_"))
      .filter(_.nonEmpty)
  }

  private def writeSqlFile(file: File, sqlText: String): Unit = {
    val writer = new PrintWriter(file, StandardCharsets.UTF_8.name())
    try {
      writer.print(sqlText)
      if (!sqlText.endsWith("\n")) {
        writer.println()
      }
    } finally {
      writer.close()
    }
  }
}
