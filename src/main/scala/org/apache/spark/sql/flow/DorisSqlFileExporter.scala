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
import java.util.Locale

import scala.util.control.NonFatal

object DorisSqlFileExporter {
  private val DefaultOutputDir = Paths.get("input/sqls")
  private val DefaultRawOutputDir = Paths.get("input/raw")
  private val DefaultOrderBy = "schedule_id ASC"
  private val WithAddJarDirSuffix = "_with_add_jar"
  private val SparkSetLinePattern = """(?im)^([ \t]*)(set\s+spark\.[^\r\n]*)$""".r
  private val AddJarLinePattern = """(?im)^[ \t]*add\s+jar\b[^\r\n]*$""".r
  private val TempFunctionLinePattern =
    """(?im)^[ \t]*create\s+(?:or\s+replace\s+)?temp(?:orary)?\s+function\b[^\r\n]*$""".r
  private[flow] val DefaultWhereClause =
    """ 1=1
      |-- and dept_name = '转转大数据研发部'
      |AND coalesce(sql, '') <> ''
      |AND job_status_id = 1
      |-- and schedule_id > 465754
      |-- and schedule_id = '536395'
      |AND job_type_id IN (3, 16)""".stripMargin

  private[flow] def defaultConfig: DorisSqlReaderConfig = {
    DorisSqlReaderConfig(
      whereClause = Some(DefaultWhereClause),
      orderBy = Some(DefaultOrderBy),
      limit = Some(100000))
  }

  def main(args: Array[String]): Unit = {
    val config = defaultConfig
    val outputDir = args.lift(1).map(Paths.get(_)).getOrElse(DefaultOutputDir)
    val files = export(config, outputDir)
    val withAddJarDir = withAddJarOutputDir(outputDir)

    // scalastyle:off println
    println(
      s"Exported ${files.size} sql files to ${outputDir.toAbsolutePath} " +
        s"and ${withAddJarDir.toAbsolutePath}")
    // scalastyle:on println
  }

  def export(
      config: DorisSqlReaderConfig = defaultConfig,
      outputDir: Path = DefaultOutputDir): Seq[Path] = {
    exportRows(DorisSqlReader.readRows(config), outputDir, DefaultRawOutputDir)
  }

  private[flow] def exportRows(
      rows: Seq[DorisSqlRow],
      outputDir: Path,
      rawOutputDir: Path = DefaultRawOutputDir): Seq[Path] = {
    val withAddJarDir = withAddJarOutputDir(outputDir)
    Files.createDirectories(outputDir)
    Files.createDirectories(withAddJarDir)
    Files.createDirectories(rawOutputDir)
    rows.map { row =>
      val file = outputDir.resolve(fileName(row))
      val withAddJarFile = withAddJarDir.resolve(fileName(row))
      val rawFile = rawOutputDir.resolve(fileName(row))
      writeSqlFile(rawFile.toFile, row.sql)
      try {
        val replacedSql = SQLVariableSubstitutor.replace(row.sql)
        val normalizedSql = normalizeExportedSql(replacedSql)
        val (targetFile, staleFile) =
          if (requiresWithAddJarDirectory(normalizedSql)) {
            (withAddJarFile, file)
          } else {
            (file, withAddJarFile)
          }
        Files.deleteIfExists(staleFile)
        writeSqlFile(targetFile.toFile, normalizedSql)
        targetFile
      } catch {
        case NonFatal(error) =>
          throw new IllegalArgumentException(
            s"Failed to export SQL file: row=${row.rowNumber}, " +
              s"job_name=${row.jobName}, output_file=${file.toAbsolutePath}, " +
              s"with_add_jar_file=${withAddJarFile.toAbsolutePath}, " +
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

  private[flow] def normalizeExportedSql(sqlText: String): String = {
    addOrderByToUnorderedRowNumber(commentOutSparkSetStatements(sqlText))
  }

  private[flow] def requiresWithAddJarDirectory(sqlText: String): Boolean = {
    val uncommentedSql = maskComments(sqlText)
    AddJarLinePattern.findFirstIn(uncommentedSql).nonEmpty ||
    TempFunctionLinePattern.findFirstIn(uncommentedSql).nonEmpty
  }

  private[flow] def commentOutSparkSetStatements(sqlText: String): String = {
    SparkSetLinePattern.replaceAllIn(sqlText, matched => {
      val indent = matched.group(1)
      val statement = matched.group(2).trim
      s"${indent}-- $statement"
    })
  }

  private[flow] def addOrderByToUnorderedRowNumber(sqlText: String): String = {
    val result = new StringBuilder
    var lastAppended = 0
    var inSingleQuote = false
    var inDoubleQuote = false
    var inBacktick = false
    var inLineComment = false
    var inBlockComment = false
    var index = 0

    while (index < sqlText.length) {
      val ch = sqlText.charAt(index)
      val next = if (index + 1 < sqlText.length) sqlText.charAt(index + 1) else 0.toChar

      if (inLineComment) {
        if (ch == '\n' || ch == '\r') {
          inLineComment = false
        }
      } else if (inBlockComment) {
        if (ch == '*' && next == '/') {
          inBlockComment = false
          index += 1
        }
      } else if (ch == '\'' && !inDoubleQuote && !inBacktick) {
        if (inSingleQuote && next == '\'') {
          index += 1
        } else {
          inSingleQuote = !inSingleQuote
        }
      } else if (ch == '"' && !inSingleQuote && !inBacktick) {
        inDoubleQuote = !inDoubleQuote
      } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
        inBacktick = !inBacktick
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '-' && next == '-') {
        inLineComment = true
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '/' && next == '*') {
        inBlockComment = true
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick &&
          matchesKeyword(sqlText, index, "row_number")) {
        extractWindowSpec(sqlText, index).foreach { spec =>
          if (shouldPatchWindowSpec(spec.content)) {
            result.append(sqlText.substring(lastAppended, spec.openParen + 1))
            result.append(patchWindowSpec(spec.content))
            lastAppended = spec.closeParen
          }
          index = spec.closeParen
        }
      }

      index += 1
    }

    if (lastAppended == 0) {
      sqlText
    } else {
      result.append(sqlText.substring(lastAppended))
      result.toString()
    }
  }

  private case class WindowSpec(openParen: Int, closeParen: Int, content: String)

  private def extractWindowSpec(sqlText: String, rowNumberIndex: Int): Option[WindowSpec] = {
    val functionArgsStart = skipWhitespace(sqlText, rowNumberIndex + "row_number".length)
    if (functionArgsStart >= sqlText.length || sqlText.charAt(functionArgsStart) != '(') {
      None
    } else {
      findMatchingParen(sqlText, functionArgsStart).flatMap { functionArgsEnd =>
        val functionArgs = sqlText.substring(functionArgsStart + 1, functionArgsEnd).trim
        if (functionArgs.nonEmpty) {
          None
        } else {
          val overIndex = skipWhitespace(sqlText, functionArgsEnd + 1)
          if (!matchesKeyword(sqlText, overIndex, "over")) {
            None
          } else {
            val specStart = skipWhitespace(sqlText, overIndex + "over".length)
            if (specStart >= sqlText.length || sqlText.charAt(specStart) != '(') {
              None
            } else {
              findMatchingParen(sqlText, specStart).map { specEnd =>
                WindowSpec(specStart, specEnd, sqlText.substring(specStart + 1, specEnd))
              }
            }
          }
        }
      }
    }
  }

  private def shouldPatchWindowSpec(spec: String): Boolean = {
    !hasTopLevelTokenSequence(spec, "order", "by") &&
      !hasTopLevelTokenSequence(spec, "sort", "by")
  }

  private def patchWindowSpec(spec: String): String = {
    val frameStart = firstTopLevelTokenPosition(spec, Set("rows", "range"))
    frameStart match {
      case Some(position) =>
        val before = spec.substring(0, position).replaceAll("\\s+$", "")
        val after = spec.substring(position).replaceAll("^\\s+", "")
        if (before.isEmpty) {
          s"ORDER BY 1 $after"
        } else {
          s"$before ORDER BY 1 $after"
        }
      case None =>
        val trimmed = spec.replaceAll("\\s+$", "")
        if (trimmed.isEmpty) {
          "ORDER BY 1"
        } else {
          s"$trimmed ORDER BY 1"
        }
    }
  }

  private def firstTopLevelTokenPosition(spec: String, targets: Set[String]): Option[Int] = {
    topLevelTokens(spec).collectFirst { case (token, position) if targets.contains(token) => position }
  }

  private def hasTopLevelTokenSequence(spec: String, first: String, second: String): Boolean = {
    topLevelTokens(spec).sliding(2).exists {
      case Seq((firstToken, _), (secondToken, _)) =>
        firstToken == first && secondToken == second
      case _ =>
        false
    }
  }

  private def topLevelTokens(spec: String): Seq[(String, Int)] = {
    val tokens = scala.collection.mutable.ArrayBuffer.empty[(String, Int)]
    var inSingleQuote = false
    var inDoubleQuote = false
    var inBacktick = false
    var inLineComment = false
    var inBlockComment = false
    var depth = 0
    var index = 0

    while (index < spec.length) {
      val ch = spec.charAt(index)
      val next = if (index + 1 < spec.length) spec.charAt(index + 1) else 0.toChar

      if (inLineComment) {
        if (ch == '\n' || ch == '\r') {
          inLineComment = false
        }
      } else if (inBlockComment) {
        if (ch == '*' && next == '/') {
          inBlockComment = false
          index += 1
        }
      } else if (ch == '\'' && !inDoubleQuote && !inBacktick) {
        if (inSingleQuote && next == '\'') {
          index += 1
        } else {
          inSingleQuote = !inSingleQuote
        }
      } else if (ch == '"' && !inSingleQuote && !inBacktick) {
        inDoubleQuote = !inDoubleQuote
      } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
        inBacktick = !inBacktick
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '-' && next == '-') {
        inLineComment = true
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '/' && next == '*') {
        inBlockComment = true
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
        if (ch == '(') {
          depth += 1
        } else if (ch == ')' && depth > 0) {
          depth -= 1
        } else if (depth == 0 && isIdentifierStart(ch)) {
          val tokenStart = index
          index += 1
          while (index < spec.length && isIdentifierPart(spec.charAt(index))) {
            index += 1
          }
          val token = spec.substring(tokenStart, index).toLowerCase(Locale.ROOT)
          tokens += token -> tokenStart
          index -= 1
        }
      }

      index += 1
    }

    tokens.toSeq
  }

  private def findMatchingParen(text: String, openParenIndex: Int): Option[Int] = {
    var inSingleQuote = false
    var inDoubleQuote = false
    var inBacktick = false
    var inLineComment = false
    var inBlockComment = false
    var depth = 0
    var index = openParenIndex

    while (index < text.length) {
      val ch = text.charAt(index)
      val next = if (index + 1 < text.length) text.charAt(index + 1) else 0.toChar

      if (inLineComment) {
        if (ch == '\n' || ch == '\r') {
          inLineComment = false
        }
      } else if (inBlockComment) {
        if (ch == '*' && next == '/') {
          inBlockComment = false
          index += 1
        }
      } else if (ch == '\'' && !inDoubleQuote && !inBacktick) {
        if (inSingleQuote && next == '\'') {
          index += 1
        } else {
          inSingleQuote = !inSingleQuote
        }
      } else if (ch == '"' && !inSingleQuote && !inBacktick) {
        inDoubleQuote = !inDoubleQuote
      } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
        inBacktick = !inBacktick
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '-' && next == '-') {
        inLineComment = true
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick && ch == '/' && next == '*') {
        inBlockComment = true
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
        if (ch == '(') {
          depth += 1
        } else if (ch == ')') {
          depth -= 1
          if (depth == 0) {
            return Some(index)
          }
        }
      }

      index += 1
    }

    None
  }

  private def skipWhitespace(text: String, from: Int): Int = {
    var index = from
    while (index < text.length && text.charAt(index).isWhitespace) {
      index += 1
    }
    index
  }

  private def matchesKeyword(text: String, index: Int, keyword: String): Boolean = {
    index >= 0 &&
      index + keyword.length <= text.length &&
      text.regionMatches(true, index, keyword, 0, keyword.length) &&
      (index == 0 || !isIdentifierPart(text.charAt(index - 1))) &&
      (index + keyword.length == text.length ||
        !isIdentifierPart(text.charAt(index + keyword.length)))
  }

  private def isIdentifierStart(ch: Char): Boolean = {
    ch.isLetter || ch == '_' || ch == '$'
  }

  private def isIdentifierPart(ch: Char): Boolean = {
    ch.isLetterOrDigit || ch == '_' || ch == '$'
  }

  private def maskComments(sqlText: String): String = {
    val result = new StringBuilder(sqlText.length)
    var inSingleQuote = false
    var inDoubleQuote = false
    var inBacktick = false
    var inLineComment = false
    var inBlockComment = false
    var index = 0

    while (index < sqlText.length) {
      val ch = sqlText.charAt(index)
      val next = if (index + 1 < sqlText.length) sqlText.charAt(index + 1) else 0.toChar

      if (inLineComment) {
        if (ch == '\n' || ch == '\r') {
          inLineComment = false
          result.append(ch)
        } else {
          result.append(' ')
        }
      } else if (inBlockComment) {
        if (ch == '*' && next == '/') {
          result.append("  ")
          inBlockComment = false
          index += 1
        } else if (ch == '\n' || ch == '\r') {
          result.append(ch)
        } else {
          result.append(' ')
        }
      } else if (ch == '\'' && !inDoubleQuote && !inBacktick) {
        result.append(ch)
        if (inSingleQuote && next == '\'') {
          result.append(next)
          index += 1
        } else {
          inSingleQuote = !inSingleQuote
        }
      } else if (ch == '"' && !inSingleQuote && !inBacktick) {
        inDoubleQuote = !inDoubleQuote
        result.append(ch)
      } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
        inBacktick = !inBacktick
        result.append(ch)
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick &&
          ch == '-' && next == '-') {
        inLineComment = true
        result.append("  ")
        index += 1
      } else if (!inSingleQuote && !inDoubleQuote && !inBacktick &&
          ch == '/' && next == '*') {
        inBlockComment = true
        result.append("  ")
        index += 1
      } else {
        result.append(ch)
      }

      index += 1
    }

    result.toString()
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

  private def withAddJarOutputDir(outputDir: Path): Path = {
    Option(outputDir.getFileName) match {
      case Some(fileName) =>
        outputDir.resolveSibling(s"${fileName.toString}$WithAddJarDirSuffix")
      case None =>
        Paths.get(s"${outputDir.toString}$WithAddJarDirSuffix")
    }
  }
}
