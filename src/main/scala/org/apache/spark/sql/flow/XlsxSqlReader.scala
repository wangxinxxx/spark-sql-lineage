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

import java.io.{File, InputStream}
import java.util.Locale
import java.util.zip.ZipFile
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}

import scala.collection.mutable

case class XlsxSqlRow(
    rowNumber: Int,
    scheduleId: String,
    scheduleUrl: String,
    sql: String)

object XlsxSqlReader {
  private val SpreadsheetNamespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
  private val RelationshipNamespace =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

  def readSqlRows(file: File): Seq[XlsxSqlRow] = {
    val zipFile = new ZipFile(file)
    try {
      val sharedStrings = readSharedStrings(zipFile)
      val sheetPath = firstSheetPath(zipFile)
      readSheetRows(zipFile, sheetPath, sharedStrings)
    } finally {
      zipFile.close()
    }
  }

  private def readSharedStrings(zipFile: ZipFile): IndexedSeq[String] = {
    val entry = zipFile.getEntry("xl/sharedStrings.xml")
    if (entry == null) {
      IndexedSeq.empty
    } else {
      val input = zipFile.getInputStream(entry)
      val reader = newXmlReader(input)
      val strings = mutable.ArrayBuffer.empty[String]
      val current = new StringBuilder
      var inSharedString = false
      var inText = false
      try {
        while (reader.hasNext) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT if reader.getLocalName == "si" =>
              inSharedString = true
              current.clear()
            case XMLStreamConstants.START_ELEMENT if inSharedString &&
                reader.getLocalName == "t" =>
              inText = true
            case XMLStreamConstants.CHARACTERS if inText =>
              current.append(reader.getText)
            case XMLStreamConstants.END_ELEMENT if reader.getLocalName == "t" =>
              inText = false
            case XMLStreamConstants.END_ELEMENT if reader.getLocalName == "si" =>
              strings += current.toString()
              inSharedString = false
            case _ =>
          }
        }
      } finally {
        reader.close()
        input.close()
      }
      strings.toIndexedSeq
    }
  }

  private def firstSheetPath(zipFile: ZipFile): String = {
    val sheetRelId = firstSheetRelId(zipFile).getOrElse("rId1")
    val target = workbookRelationships(zipFile).getOrElse(sheetRelId, "worksheets/sheet1.xml")
    if (target.startsWith("/")) {
      target.stripPrefix("/")
    } else if (target.startsWith("xl/")) {
      target
    } else {
      s"xl/$target"
    }
  }

  private def firstSheetRelId(zipFile: ZipFile): Option[String] = {
    val entry = zipFile.getEntry("xl/workbook.xml")
    if (entry == null) {
      None
    } else {
      val input = zipFile.getInputStream(entry)
      val reader = newXmlReader(input)
      try {
        var relId: Option[String] = None
        while (reader.hasNext && relId.isEmpty) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT if reader.getLocalName == "sheet" =>
              relId = Option(reader.getAttributeValue(RelationshipNamespace, "id"))
            case _ =>
          }
        }
        relId
      } finally {
        reader.close()
        input.close()
      }
    }
  }

  private def workbookRelationships(zipFile: ZipFile): Map[String, String] = {
    val entry = zipFile.getEntry("xl/_rels/workbook.xml.rels")
    if (entry == null) {
      Map.empty
    } else {
      val input = zipFile.getInputStream(entry)
      val reader = newXmlReader(input)
      val relationships = mutable.Map.empty[String, String]
      try {
        while (reader.hasNext) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT if reader.getLocalName == "Relationship" =>
              val id = reader.getAttributeValue(null, "Id")
              val target = reader.getAttributeValue(null, "Target")
              if (id != null && target != null) {
                relationships += id -> target
              }
            case _ =>
          }
        }
      } finally {
        reader.close()
        input.close()
      }
      relationships.toMap
    }
  }

  private def readSheetRows(
      zipFile: ZipFile,
      sheetPath: String,
      sharedStrings: IndexedSeq[String]): Seq[XlsxSqlRow] = {
    val entry = zipFile.getEntry(sheetPath)
    if (entry == null) {
      throw new IllegalArgumentException(s"Cannot find xlsx sheet: $sheetPath")
    }

    val input = zipFile.getInputStream(entry)
    val reader = newXmlReader(input)
    val sqlRows = mutable.ArrayBuffer.empty[XlsxSqlRow]
    val currentCells = mutable.Map.empty[Int, String]
    val currentText = new StringBuilder
    var headers = Map.empty[String, Int]
    var currentRowNumber = 0
    var currentCellColumn = 0
    var currentCellType = ""
    var inCell = false
    var collectingText = false

    try {
      while (reader.hasNext) {
        reader.next() match {
          case XMLStreamConstants.START_ELEMENT if reader.getLocalName == "row" =>
            currentRowNumber = Option(reader.getAttributeValue(null, "r")).map(_.toInt).getOrElse(0)
            currentCells.clear()

          case XMLStreamConstants.START_ELEMENT if reader.getLocalName == "c" =>
            inCell = true
            currentText.clear()
            currentCellType = Option(reader.getAttributeValue(null, "t")).getOrElse("")
            currentCellColumn = columnIndex(reader.getAttributeValue(null, "r"))

          case XMLStreamConstants.START_ELEMENT if inCell &&
              (reader.getLocalName == "v" || reader.getLocalName == "t") =>
            collectingText = true

          case XMLStreamConstants.CHARACTERS if collectingText =>
            currentText.append(reader.getText)

          case XMLStreamConstants.END_ELEMENT if reader.getLocalName == "v" ||
              reader.getLocalName == "t" =>
            collectingText = false

          case XMLStreamConstants.END_ELEMENT if reader.getLocalName == "c" =>
            val value = cellValue(currentText.toString(), currentCellType, sharedStrings)
            currentCells += currentCellColumn -> value
            inCell = false

          case XMLStreamConstants.END_ELEMENT if reader.getLocalName == "row" =>
            if (headers.isEmpty) {
              headers = headerIndexes(currentCells.toMap)
            } else {
              sqlRow(currentRowNumber, currentCells.toMap, headers).foreach(sqlRows += _)
            }

          case _ =>
        }
      }
    } finally {
      reader.close()
      input.close()
    }

    sqlRows.toSeq
  }

  private def cellValue(
      rawValue: String,
      cellType: String,
      sharedStrings: IndexedSeq[String]): String = {
    cellType match {
      case "s" if rawValue.nonEmpty =>
        sharedStrings.lift(rawValue.toInt).getOrElse("")
      case _ =>
        rawValue
    }
  }

  private def headerIndexes(cells: Map[Int, String]): Map[String, Int] = {
    cells.map { case (column, value) =>
      value.trim.toLowerCase(Locale.ROOT) -> column
    }
  }

  private def sqlRow(
      rowNumber: Int,
      cells: Map[Int, String],
      headers: Map[String, Int]): Option[XlsxSqlRow] = {
    val sqlColumn = headers.getOrElse("sql", 3)
    val sql = cells.getOrElse(sqlColumn, "").trim
    if (sql.isEmpty) {
      None
    } else {
      Some(XlsxSqlRow(
        rowNumber,
        headers.get("schedule_id").flatMap(cells.get).getOrElse(""),
        headers.get("schedule_url").flatMap(cells.get).getOrElse(""),
        sql))
    }
  }

  private def columnIndex(cellReference: String): Int = {
    if (cellReference == null || cellReference.isEmpty) {
      0
    } else {
      cellReference.takeWhile(_.isLetter).foldLeft(0) { case (index, ch) =>
        index * 26 + ch.toUpper - 'A' + 1
      }
    }
  }

  private def newXmlReader(input: InputStream): XMLStreamReader = {
    val factory = XMLInputFactory.newInstance()
    try {
      factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    } catch {
      case _: IllegalArgumentException =>
    }
    try {
      factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false)
    } catch {
      case _: IllegalArgumentException =>
    }
    factory.createXMLStreamReader(input)
  }
}
