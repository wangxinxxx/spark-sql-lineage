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

import java.time.{DayOfWeek, Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.{TemporalAdjusters, WeekFields}
import java.util.Locale

import scala.util.matching.Regex

case class SQLVariableContext(
    runDateTime: LocalDateTime = LocalDateTime.now(),
    selectedDateTime: Option[LocalDateTime] = None,
    zoneId: ZoneId = ZoneId.systemDefault(),
    tempCatalog: String = "/tmp/dw_tmp_file/",
    moduleName: Option[String] = None,
    overrides: Map[String, String] = Map.empty) {

  def effectiveDateTime: LocalDateTime = selectedDateTime.getOrElse(runDateTime)

  def today: LocalDate = effectiveDateTime.toLocalDate

  def businessDate: LocalDate = today.minusDays(1)
}

object SQLVariableSubstitutor {
  private val VariablePattern = """\$\{([^}]+)\}""".r
  private val BashPattern = """\$bash\{([^}]*)\}""".r
  private val HashStringVariablePattern =
    """(['"])#(year|month|day|hour|minute|second)#\1""".r
  private val HashNumericVariablePattern = """#(year|month|day|hour|minute|second)#""".r
  private val DateFunctionPattern = """#date\(([^#]*)\):([^#]+)#""".r
  private val SetVariablePattern =
    """(?ims)(?:^|;)\s*set\s+(?:hivevar:|hiveconf:)?(@?[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)(?=;)""".r
  private val AtVariableReferencePattern = """@([A-Za-z_][A-Za-z0-9_]*)""".r
  private val HiveVariablePrefixPattern = """(?i)^(hivevar|hiveconf):(.+)$""".r
  private val BashOffsetUnits = "year|month|week|day|hour|minute|min|second|sec"
  private val BashOffsetPattern =
    ("""(?i)^\s*([+-]?\s*\d+)\s*(?:[+-]\s*)?(""" + BashOffsetUnits + """)s?\s*$""").r
  private val BashOffsetFindPattern =
    ("""(?i)([+-]?\s*\d+)\s*(?:[+-]\s*)?(""" + BashOffsetUnits + """)s?\b""").r
  private val DateTimeFindPattern =
    """(\d{4}-\d{2}-\d{2}|\d{8})(?:[ T](\d{2}:\d{2}:\d{2}))?""".r
  private val BashRoundedEpochPattern =
    """(?is)^\s*date\s+-d\s+["']?@\$\(\(\s*\$\(date\s+\+%s\)\s*/\s*(\d+)\s*\*\s*(\d+)(?:\s*([+-])\s*(\d+))?\s*\)\)["']?\s+\+(.+?)\s*$""".r
  private val BashMidnightEpochOffsetPattern =
    """(?is)^\s*date\s+-d\s+["']?@\$\(\(\s*\$\(date\s+-d\s+"\$\(date\s+\+%Y-%m-%d\)\s+00:00:00"\s+\+%s\)\s*([+-])\s*(\d+)\s*\)\)["']?\s+\+(.+?)\s*$""".r

  private val DateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)
  private val DateSuffixFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ROOT)
  private val MonthSuffixFormatter = DateTimeFormatter.ofPattern("yyyyMM", Locale.ROOT)
  private val HourSuffixFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH", Locale.ROOT)
  private val DateTimeFormatterText =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT)

  def replace(sqlText: String, context: SQLVariableContext = SQLVariableContext()): String = {
    val setVariables = extractSetVariables(sqlText)
    val withVariables = replaceVariables(sqlText, context, setVariables, Set.empty)
    val withAtVariables = replaceAtVariables(withVariables, context, setVariables)
    BashPattern.replaceAllIn(withAtVariables, matched =>
      Regex.quoteReplacement(evaluateBashDate(matched.group(1), context)))
  }

  private def replaceVariables(
      sqlText: String,
      context: SQLVariableContext,
      setVariables: Map[String, String],
      resolvingSetVariables: Set[String]): String = {
    VariablePattern.replaceAllIn(sqlText, matched => {
      Regex.quoteReplacement(
        replaceVariable(matched.group(1), context, setVariables, resolvingSetVariables)
          .getOrElse(matched.matched))
    })
  }

  private def replaceVariable(
      content: String,
      context: SQLVariableContext,
      setVariables: Map[String, String],
      resolvingSetVariables: Set[String]): Option[String] = {
    val key = content.trim
    val lookupKeys = variableLookupKeys(key)
    lookupVariable(context.overrides, lookupKeys).map(_._2)
      .orElse(lookupVariable(setVariables, lookupKeys).map { case (resolvedKey, value) =>
        if (resolvingSetVariables.contains(resolvedKey)) {
          value
        } else {
          replaceVariables(value, context, setVariables, resolvingSetVariables + resolvedKey)
        }
      })
      .orElse(namedVariables(context).get(key))
      .orElse {
        if (isHiveVariableReference(key)) {
          None
        } else if (looksLikeDateFormat(key)) {
          Some(formatDatePattern(key, context))
        } else if (looksLikeFormula(key)) {
          Some(evaluateFormula(key, context))
        } else {
          None
        }
      }
  }

  private def extractSetVariables(sqlText: String): Map[String, String] = {
    SetVariablePattern.findAllMatchIn(sqlText).flatMap { matched =>
      val variableName = matched.group(1).trim.stripPrefix("@")
      val value = matched.group(2).trim
      if (variableName.nonEmpty) {
        Some(variableName -> value)
      } else {
        None
      }
    }.flatMap { case (key, value) =>
      Seq(
        key -> value,
        s"@$key" -> value,
        s"hivevar:$key" -> value,
        s"hiveconf:$key" -> value)
    }.toMap
  }

  private def replaceAtVariables(
      sqlText: String,
      context: SQLVariableContext,
      setVariables: Map[String, String]): String = {
    AtVariableReferencePattern.replaceAllIn(sqlText, matched => {
      val start = matched.start
      if (isSetVariableDeclarationAt(sqlText, start)) {
        matched.matched
      } else {
        val name = matched.group(1)
        val value = lookupVariable(setVariables, Seq(s"@$name", name)).map(_._2)
        Regex.quoteReplacement(value.map { text =>
          replaceVariables(text, context, setVariables, Set(s"@$name", name))
        }.getOrElse(matched.matched))
      }
    })
  }

  private def isSetVariableDeclarationAt(sqlText: String, atIndex: Int): Boolean = {
    val previousSemicolon = sqlText.lastIndexOf(';', atIndex - 1)
    val previousLineBreak = sqlText.lastIndexOf('\n', atIndex - 1)
    val segmentStart = math.max(previousSemicolon, previousLineBreak) + 1
    sqlText.substring(segmentStart, atIndex).trim.equalsIgnoreCase("set")
  }

  private def lookupVariable(
      variables: Map[String, String],
      keys: Seq[String]): Option[(String, String)] = {
    keys.collectFirst {
      case key if variables.contains(key) => key -> variables(key)
    }
  }

  private def variableLookupKeys(key: String): Seq[String] = key match {
    case HiveVariablePrefixPattern(_, variableName) =>
      val name = variableName.trim
      Seq(key, name, s"@$name", s"hivevar:$name", s"hiveconf:$name")
    case _ =>
      Seq(key, s"@$key", s"hivevar:$key", s"hiveconf:$key")
  }

  private def isHiveVariableReference(key: String): Boolean = key match {
    case HiveVariablePrefixPattern(_, _) => true
    case _ => false
  }

  private def namedVariables(context: SQLVariableContext): Map[String, String] = {
    val businessDate = context.businessDate
    val today = context.today
    val now = context.effectiveDateTime
    val monthBegin = businessDate.`with`(TemporalAdjusters.firstDayOfMonth())
    val monthEnd = businessDate.`with`(TemporalAdjusters.lastDayOfMonth())
    val quarterBegin = startOfQuarter(businessDate)
    val quarterEnd = quarterBegin.plusMonths(2).`with`(TemporalAdjusters.lastDayOfMonth())
    val weekBegin = weekBoundary(businessDate, DayOfWeek.SUNDAY, begin = true)
    val weekEnd = weekBoundary(businessDate, DayOfWeek.SATURDAY, begin = false)
    val weekBeginCn = weekBoundary(businessDate, DayOfWeek.MONDAY, begin = true)
    val weekEndCn = weekBoundary(businessDate, DayOfWeek.SUNDAY, begin = false)
    val moduleName = context.moduleName.getOrElse("module_name")
    val dataFilePrefix = context.tempCatalog.stripSuffix("/")

    Map(
      "tempCatalog" -> context.tempCatalog,
      "outFileSuffix" -> formatDate(businessDate),
      "startDate" -> quoted(formatDate(businessDate)),
      "dateSuffix" -> businessDate.format(DateSuffixFormatter),
      "dateHourSuffix" -> now.format(HourSuffixFormatter),
      "dateBeforeOneHourSuffix" -> now.minusHours(1).format(HourSuffixFormatter),
      "monthSuffix" -> businessDate.format(MonthSuffixFormatter),
      "lastMonthSuffix" -> businessDate.minusMonths(1).format(MonthSuffixFormatter),
      "today" -> quoted(formatDate(today)),
      "todayDateTime" -> now.format(DateTimeFormatterText),
      "todaySuffix" -> today.format(DateSuffixFormatter),
      "dealDate" -> quoted(formatDate(businessDate)),
      "dealTimestamp" -> businessDate.atStartOfDay(context.zoneId).toInstant.toEpochMilli.toString,
      "monthId" -> quoted(f"${businessDate.getYear}%04dM${businessDate.getMonthValue}%02d"),
      "monthBegin" -> quoted(formatDate(monthBegin)),
      "monthEnd" -> quoted(formatDate(monthEnd)),
      "monthBeginSuffix" -> monthBegin.format(DateSuffixFormatter),
      "monthEndSuffix" -> monthEnd.format(DateSuffixFormatter),
      "quarterBegin" -> formatDate(quarterBegin),
      "quarterEnd" -> formatDate(quarterEnd),
      "quarterBeginSuffix" -> quarterBegin.format(DateSuffixFormatter),
      "quarterEndSuffix" -> quarterEnd.format(DateSuffixFormatter),
      "weekId" -> quoted(weekId(businessDate, DayOfWeek.SUNDAY)),
      "weekIdCn" -> quoted(weekId(businessDate, DayOfWeek.MONDAY)),
      "weekBegin" -> quoted(formatDate(weekBegin)),
      "weekEnd" -> quoted(formatDate(weekEnd)),
      "weekBeginSuffix" -> weekBegin.format(DateSuffixFormatter),
      "weekEndSuffix" -> weekEnd.format(DateSuffixFormatter),
      "weekBeginCn" -> quoted(formatDate(weekBeginCn)),
      "weekEndCn" -> quoted(formatDate(weekEndCn)),
      "weekBeginCnSuffix" -> weekBeginCn.format(DateSuffixFormatter),
      "weekEndCnSuffix" -> weekEndCn.format(DateSuffixFormatter),
      "sevenDaysBefore" -> formatDate(businessDate.minusDays(7)),
      "sevenDaysBeforeSuffix" -> businessDate.minusDays(7).format(DateSuffixFormatter),
      "thirtyDaysBeforeSuffix" -> businessDate.minusDays(30).format(DateSuffixFormatter),
      "sixtyDaysBeforeSuffix" -> businessDate.minusDays(60).format(DateSuffixFormatter),
      "monthOnlySuffix" -> f"${businessDate.getMonthValue}%02d",
      "dataFile" -> s"$dataFilePrefix/$moduleName/data")
  }

  private def looksLikeFormula(content: String): Boolean = {
    val withoutStringLiterals = stripStringLiterals(content)
    content.contains("#") ||
      withoutStringLiterals.matches(".*\\b(year|month|day|hour|minute|second)\\b.*") ||
      looksLikePureNumericFormula(withoutStringLiterals)
  }

  private def looksLikePureNumericFormula(content: String): Boolean = {
    content.exists(ch => "+-*/%?:<>=&|!".contains(ch)) &&
      content.forall(ch => ch.isDigit || ch.isWhitespace || "()+-*/%?:<>=&|!".contains(ch))
  }

  private def stripStringLiterals(content: String): String = {
    val builder = new StringBuilder
    var quote: Option[Char] = None
    var index = 0
    while (index < content.length) {
      val ch = content.charAt(index)
      quote match {
        case Some(q) if ch == q =>
          quote = None
        case Some(_) =>
        case None if ch == '\'' || ch == '"' =>
          quote = Some(ch)
        case None =>
          builder.append(ch)
      }
      index += 1
    }
    builder.toString()
  }

  private def looksLikeDateFormat(content: String): Boolean = {
    !content.contains("#") &&
      content.contains("yyyy") &&
      content.exists(ch => "MdHhmsS".contains(ch)) &&
      content.forall(ch => ch.isLetterOrDigit || " -_:/.".contains(ch))
  }

  private def formatDatePattern(pattern: String, context: SQLVariableContext): String = {
    val formatter = DateTimeFormatter.ofPattern(pattern, Locale.ROOT)
    context.businessDate.atStartOfDay().format(formatter)
  }

  private def evaluateFormula(content: String, context: SQLVariableContext): String = {
    val variables = formulaVariables(context)
    val withHashVariables = replaceHashVariables(content, context)
    val withDateFunctions = DateFunctionPattern.replaceAllIn(withHashVariables, matched => {
      val formatted = evaluateDateFunction(matched.group(1), matched.group(2), context)
      Regex.quoteReplacement(quoted(formatted))
    })
    val parser = new FormulaParser(withDateFunctions, variables)
    parser.parse().render
  }

  private def replaceHashVariables(content: String, context: SQLVariableContext): String = {
    HashStringVariablePattern.replaceAllIn(content, matched => {
      val variable = matched.group(2)
      Regex.quoteReplacement(quoted(paddedFormulaValue(variable, context)))
    }) match {
      case withStringVariables =>
        HashNumericVariablePattern.replaceAllIn(withStringVariables, matched => {
          val variable = matched.group(1)
          Regex.quoteReplacement(formulaVariables(context)(variable).toString)
        })
    }
  }

  private def evaluateDateFunction(
      argumentsText: String,
      formatText: String,
      context: SQLVariableContext): String = {
    val variables = formulaVariables(context)
    val increments = splitTopLevel(argumentsText).map { argument =>
      if (argument.trim.isEmpty) {
        0L
      } else {
        new FormulaParser(argument, variables).parse().asLong
      }
    }.padTo(6, 0L)
    val dateTime = context.effectiveDateTime
      .plusYears(increments(0))
      .plusMonths(increments(1))
      .plusDays(increments(2))
      .plusHours(increments(3))
      .plusMinutes(increments(4))
      .plusSeconds(increments(5))
    dateTime.format(DateTimeFormatter.ofPattern(formatText, Locale.ROOT))
  }

  private def evaluateBashDate(commandText: String, context: SQLVariableContext): String = {
    evaluateRoundedEpochBashDate(commandText, context).getOrElse {
      evaluateSimpleBashDate(commandText, context)
    }
  }

  private def evaluateRoundedEpochBashDate(
      commandText: String,
      context: SQLVariableContext): Option[String] = {
    commandText match {
      case BashRoundedEpochPattern(divisorText, multiplierText, sign, offsetText, formatPart)
          if divisorText == multiplierText =>
        val intervalSeconds = divisorText.toLong
        val epochSeconds = context.effectiveDateTime.atZone(context.zoneId).toEpochSecond
        val offsetSeconds = signedSeconds(Option(sign), Option(offsetText))
        Some(formatEpochSeconds(
          epochSeconds / intervalSeconds * intervalSeconds + offsetSeconds,
          formatPart,
          context))
      case BashRoundedEpochPattern(divisorText, multiplierText, _, _, _) =>
        throw new IllegalArgumentException(
          s"Unsupported bash rounded date interval: / $divisorText * $multiplierText")
      case BashMidnightEpochOffsetPattern(sign, offsetText, formatPart) =>
        val midnightEpochSeconds = context.today.atStartOfDay(context.zoneId).toEpochSecond
        Some(formatEpochSeconds(
          midnightEpochSeconds + signedSeconds(Some(sign), Some(offsetText)),
          formatPart,
          context))
      case _ =>
        None
    }
  }

  private def signedSeconds(sign: Option[String], secondsText: Option[String]): Long = {
    secondsText.map { text =>
      val seconds = text.toLong
      sign match {
        case Some("-") => -seconds
        case _ => seconds
      }
    }.getOrElse(0L)
  }

  private def formatEpochSeconds(
      epochSeconds: Long,
      formatPart: String,
      context: SQLVariableContext): String = {
    val dateTime = LocalDateTime.ofInstant(
      Instant.ofEpochSecond(epochSeconds),
      context.zoneId)
    val formatText = "+" + stripQuotes(formatPart.trim)
    dateTime.format(DateTimeFormatter.ofPattern(toJavaDatePattern(formatText), Locale.ROOT))
  }

  private def evaluateSimpleBashDate(commandText: String, context: SQLVariableContext): String = {
    val tokens = splitShellWords(commandText)
    if (tokens.headOption.forall(_ != "date")) {
      throw new IllegalArgumentException(s"Unsupported bash variable expression: $commandText")
    }

    val formatText = tokens.find(_.startsWith("+")).getOrElse {
      throw new IllegalArgumentException(s"Unsupported bash date format: $commandText")
    }
    val dateText = tokens.sliding(2).find(_.head == "-d").map(_(1))
    val dateTime = dateText
      .map(evaluateBashDateText(_, context))
      .getOrElse(context.effectiveDateTime)
    dateTime.format(DateTimeFormatter.ofPattern(toJavaDatePattern(formatText), Locale.ROOT))
  }

  private def evaluateBashDateText(
      dateText: String,
      context: SQLVariableContext): LocalDateTime = {
    val dateMatch = DateTimeFindPattern.findFirstMatchIn(dateText)
    val baseDateTime = dateMatch.map { matched =>
      val date = parseBashDate(matched.group(1))
      Option(matched.group(2)).map { timeText =>
        LocalDateTime.of(date, java.time.LocalTime.parse(timeText))
      }.getOrElse(date.atStartOfDay())
    }.getOrElse(context.effectiveDateTime)

    val withoutDate = dateMatch.map { matched =>
      dateText.substring(0, matched.start) + dateText.substring(matched.end)
    }.getOrElse(dateText)
    val offsets = BashOffsetFindPattern.findAllMatchIn(withoutDate).toSeq
    val residue = BashOffsetFindPattern.replaceAllIn(withoutDate, "").trim

    if (residue.nonEmpty && residue != "now" && residue != "today") {
      throw new IllegalArgumentException(s"Unsupported bash date offset: $dateText")
    }

    offsets.foldLeft(baseDateTime) { case (dateTime, matched) =>
      applyBashOffset(dateTime, s"${matched.group(1)} ${matched.group(2)}")
    }
  }

  private def parseBashDate(dateText: String): LocalDate = {
    if (dateText.contains("-")) {
      LocalDate.parse(dateText, DateFormatter)
    } else {
      LocalDate.parse(dateText, DateSuffixFormatter)
    }
  }

  private def splitShellWords(text: String): Seq[String] = {
    val tokens = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var quote: Option[Char] = None
    var index = 0
    while (index < text.length) {
      val ch = text.charAt(index)
      quote match {
        case Some(q) if ch == q =>
          quote = None
        case Some(_) =>
          current.append(ch)
        case None if ch == '\'' || ch == '"' =>
          quote = Some(ch)
        case None if ch.isWhitespace =>
          if (current.nonEmpty) {
            tokens += current.toString()
            current.clear()
          }
        case None =>
          current.append(ch)
      }
      index += 1
    }
    if (quote.nonEmpty) {
      throw new IllegalArgumentException(s"Unclosed quote in bash date expression: $text")
    }
    if (current.nonEmpty) {
      tokens += current.toString()
    }
    tokens.toSeq
  }

  private def applyBashOffset(dateTime: LocalDateTime, offsetText: String): LocalDateTime = {
    offsetText match {
      case BashOffsetPattern(amountText, unit) =>
        val amount = amountText.replaceAll("\\s+", "").toLong
        unit.toLowerCase(Locale.ROOT) match {
          case "year" => dateTime.plusYears(amount)
          case "month" => dateTime.plusMonths(amount)
          case "week" => dateTime.plusWeeks(amount)
          case "day" => dateTime.plusDays(amount)
          case "hour" => dateTime.plusHours(amount)
          case "minute" | "min" => dateTime.plusMinutes(amount)
          case "second" | "sec" => dateTime.plusSeconds(amount)
        }
      case "now" | "today" =>
        dateTime
      case _ =>
        throw new IllegalArgumentException(s"Unsupported bash date offset: $offsetText")
    }
  }

  private def toJavaDatePattern(gnuFormatText: String): String = {
    val formatText = gnuFormatText.stripPrefix("+")
    val builder = new StringBuilder
    var index = 0
    while (index < formatText.length) {
      val ch = formatText.charAt(index)
      if (ch == '%' && index + 1 < formatText.length) {
        index += 1
        builder.append(gnuDateToken(formatText.charAt(index)))
      } else {
        appendDatePatternLiteral(builder, ch)
      }
      index += 1
    }
    builder.toString()
  }

  private def gnuDateToken(token: Char): String = token match {
    case 'Y' => "yyyy"
    case 'y' => "yy"
    case 'm' => "MM"
    case 'd' => "dd"
    case 'H' => "HH"
    case 'M' => "mm"
    case 'S' => "ss"
    case 'F' => "yyyy-MM-dd"
    case 'T' => "HH:mm:ss"
    case '%' => "%"
    case other => datePatternLiteral(other)
  }

  private def appendDatePatternLiteral(builder: StringBuilder, ch: Char): Unit = {
    if (ch.isLetter) {
      builder.append(datePatternLiteral(ch))
    } else {
      builder.append(ch)
    }
  }

  private def datePatternLiteral(ch: Char): String = ch match {
    case '\'' => "''"
    case other => s"'$other'"
  }

  private def splitTopLevel(text: String): Seq[String] = {
    val parts = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var depth = 0
    text.foreach {
      case ',' if depth == 0 =>
        parts += current.toString()
        current.clear()
      case '(' =>
        depth += 1
        current.append('(')
      case ')' =>
        depth -= 1
        current.append(')')
      case ch =>
        current.append(ch)
    }
    parts += current.toString()
    parts.toSeq
  }

  private def startOfQuarter(date: LocalDate): LocalDate = {
    val month = ((date.getMonthValue - 1) / 3) * 3 + 1
    LocalDate.of(date.getYear, month, 1)
  }

  private def weekBoundary(date: LocalDate, dayOfWeek: DayOfWeek, begin: Boolean): LocalDate = {
    val boundary = if (begin) {
      date.`with`(TemporalAdjusters.previousOrSame(dayOfWeek))
    } else {
      date.`with`(TemporalAdjusters.nextOrSame(dayOfWeek))
    }
    if (boundary.getYear < date.getYear) {
      LocalDate.of(date.getYear, 1, 1)
    } else if (boundary.getYear > date.getYear) {
      LocalDate.of(date.getYear, 12, 31)
    } else {
      boundary
    }
  }

  private def weekId(date: LocalDate, firstDayOfWeek: DayOfWeek): String = {
    val weekFields = WeekFields.of(firstDayOfWeek, 1)
    val week = date.get(weekFields.weekOfWeekBasedYear())
    val year = date.get(weekFields.weekBasedYear())
    f"$year%04dW$week%02d"
  }

  private def formulaVariables(context: SQLVariableContext): Map[String, Long] = {
    val dateTime = context.effectiveDateTime
    Map(
      "year" -> dateTime.getYear.toLong,
      "month" -> dateTime.getMonthValue.toLong,
      "day" -> dateTime.getDayOfMonth.toLong,
      "hour" -> dateTime.getHour.toLong,
      "minute" -> dateTime.getMinute.toLong,
      "second" -> dateTime.getSecond.toLong)
  }

  private def paddedFormulaValue(variable: String, context: SQLVariableContext): String = {
    val value = formulaVariables(context)(variable)
    variable match {
      case "year" => f"$value%04d"
      case _ => f"$value%02d"
    }
  }

  private def formatDate(date: LocalDate): String = date.format(DateFormatter)

  private def quoted(value: String): String = s"'$value'"

  private def stripQuotes(value: String): String = {
    val trimmed = value.trim
    if (trimmed.length >= 2 &&
        ((trimmed.head == '\'' && trimmed.last == '\'') ||
          (trimmed.head == '"' && trimmed.last == '"'))) {
      trimmed.substring(1, trimmed.length - 1)
    } else {
      trimmed
    }
  }

  private sealed trait FormulaValue {
    def asLong: Long

    def asBoolean: Boolean

    def asString: String

    def render: String = asString
  }

  private case class NumericFormulaValue(value: Long) extends FormulaValue {
    override def asLong: Long = value

    override def asBoolean: Boolean = value != 0

    override def asString: String = value.toString
  }

  private case class StringFormulaValue(value: String) extends FormulaValue {
    override def asLong: Long = value.toLong

    override def asBoolean: Boolean = value.nonEmpty

    override def asString: String = value
  }

  private case class BooleanFormulaValue(value: Boolean) extends FormulaValue {
    override def asLong: Long = if (value) 1L else 0L

    override def asBoolean: Boolean = value

    override def asString: String = value.toString
  }

  private sealed trait FormulaToken

  private case class NumberToken(value: Long) extends FormulaToken

  private case class StringToken(value: String) extends FormulaToken

  private case class IdentifierToken(value: String) extends FormulaToken

  private case class SymbolToken(value: String) extends FormulaToken

  private case object EndToken extends FormulaToken

  private class FormulaParser(expression: String, variables: Map[String, Long]) {
    private val tokens = tokenize(expression)
    private var position = 0

    def parse(): FormulaValue = {
      val value = parseTernary()
      expectEnd()
      value
    }

    private def parseTernary(): FormulaValue = {
      val condition = parseOr()
      if (accept("?")) {
        val trueValue = parseTernary()
        expect(":")
        val falseValue = parseTernary()
        if (condition.asBoolean) trueValue else falseValue
      } else {
        condition
      }
    }

    private def parseOr(): FormulaValue = {
      var left = parseAnd()
      while (accept("||")) {
        val right = parseAnd()
        left = BooleanFormulaValue(left.asBoolean || right.asBoolean)
      }
      left
    }

    private def parseAnd(): FormulaValue = {
      var left = parseEquality()
      while (accept("&&")) {
        val right = parseEquality()
        left = BooleanFormulaValue(left.asBoolean && right.asBoolean)
      }
      left
    }

    private def parseEquality(): FormulaValue = {
      var left = parseComparison()
      while (peek("==") || peek("!=")) {
        if (accept("==")) {
          left = BooleanFormulaValue(compareEqual(left, parseComparison()))
        } else {
          expect("!=")
          left = BooleanFormulaValue(!compareEqual(left, parseComparison()))
        }
      }
      left
    }

    private def parseComparison(): FormulaValue = {
      var left = parseAdd()
      while (peek("<") || peek("<=") || peek(">") || peek(">=")) {
        if (accept("<")) {
          left = BooleanFormulaValue(compareNumeric(left, parseAdd()) < 0)
        } else if (accept("<=")) {
          left = BooleanFormulaValue(compareNumeric(left, parseAdd()) <= 0)
        } else if (accept(">")) {
          left = BooleanFormulaValue(compareNumeric(left, parseAdd()) > 0)
        } else {
          expect(">=")
          left = BooleanFormulaValue(compareNumeric(left, parseAdd()) >= 0)
        }
      }
      left
    }

    private def parseAdd(): FormulaValue = {
      var left = parseMultiply()
      while (peek("+") || peek("-")) {
        if (accept("+")) {
          val right = parseMultiply()
          left = (left, right) match {
            case (_: StringFormulaValue, _) | (_, _: StringFormulaValue) =>
              StringFormulaValue(left.asString + right.asString)
            case _ =>
              NumericFormulaValue(left.asLong + right.asLong)
          }
        } else {
          expect("-")
          left = NumericFormulaValue(left.asLong - parseMultiply().asLong)
        }
      }
      left
    }

    private def parseMultiply(): FormulaValue = {
      var left = parseUnary()
      while (peek("*") || peek("/") || peek("%")) {
        if (accept("*")) {
          left = NumericFormulaValue(left.asLong * parseUnary().asLong)
        } else if (accept("/")) {
          left = NumericFormulaValue(left.asLong / parseUnary().asLong)
        } else {
          expect("%")
          left = NumericFormulaValue(left.asLong % parseUnary().asLong)
        }
      }
      left
    }

    private def parseUnary(): FormulaValue = {
      if (accept("-")) {
        NumericFormulaValue(-parseUnary().asLong)
      } else if (accept("!")) {
        BooleanFormulaValue(!parseUnary().asBoolean)
      } else {
        parsePrimary()
      }
    }

    private def parsePrimary(): FormulaValue = current match {
      case NumberToken(value) =>
        position += 1
        NumericFormulaValue(value)
      case StringToken(value) =>
        position += 1
        StringFormulaValue(value)
      case IdentifierToken(value) =>
        position += 1
        NumericFormulaValue(variables.getOrElse(value,
          throw new IllegalArgumentException(s"Unknown formula variable: $value")))
      case SymbolToken("(") =>
        position += 1
        val value = parseTernary()
        expect(")")
        value
      case token =>
        throw new IllegalArgumentException(s"Unexpected token in formula: $token")
    }

    private def compareEqual(left: FormulaValue, right: FormulaValue): Boolean = {
      (left, right) match {
        case (_: StringFormulaValue, _) | (_, _: StringFormulaValue) =>
          left.asString == right.asString
        case _ =>
          left.asLong == right.asLong
      }
    }

    private def compareNumeric(left: FormulaValue, right: FormulaValue): Int = {
      java.lang.Long.compare(left.asLong, right.asLong)
    }

    private def accept(symbol: String): Boolean = {
      if (peek(symbol)) {
        position += 1
        true
      } else {
        false
      }
    }

    private def expect(symbol: String): Unit = {
      if (!accept(symbol)) {
        throw new IllegalArgumentException(s"Expected `$symbol` in formula: $expression")
      }
    }

    private def expectEnd(): Unit = {
      if (current != EndToken) {
        throw new IllegalArgumentException(s"Unexpected token after formula: $current")
      }
    }

    private def peek(symbol: String): Boolean = current == SymbolToken(symbol)

    private def current: FormulaToken = tokens(position)

    private def tokenize(text: String): Vector[FormulaToken] = {
      val result = Vector.newBuilder[FormulaToken]
      var index = 0
      while (index < text.length) {
        text.charAt(index) match {
          case ch if ch.isWhitespace =>
            index += 1
          case ch if ch.isDigit =>
            val start = index
            while (index < text.length && text.charAt(index).isDigit) {
              index += 1
            }
            result += NumberToken(text.substring(start, index).toLong)
          case '\'' | '"' =>
            val quote = text.charAt(index)
            val parsed = readString(text, index + 1, quote)
            result += StringToken(parsed._1)
            index = parsed._2
          case ch if ch.isLetter || ch == '_' =>
            val start = index
            while (index < text.length &&
                (text.charAt(index).isLetterOrDigit || text.charAt(index) == '_')) {
              index += 1
            }
            result += IdentifierToken(text.substring(start, index))
          case _ =>
            val twoChar = if (index + 1 < text.length) text.substring(index, index + 2) else ""
            if (Set("&&", "||", "==", "!=", "<=", ">=").contains(twoChar)) {
              result += SymbolToken(twoChar)
              index += 2
            } else {
              result += SymbolToken(text.charAt(index).toString)
              index += 1
            }
        }
      }
      result += EndToken
      result.result()
    }

    private def readString(text: String, start: Int, quote: Char): (String, Int) = {
      val builder = new StringBuilder
      var index = start
      var closed = false
      while (index < text.length && !closed) {
        val ch = text.charAt(index)
        if (ch == '\\' && index + 1 < text.length) {
          builder.append(text.charAt(index + 1))
          index += 2
        } else if (ch == quote) {
          index += 1
          closed = true
        } else {
          builder.append(ch)
          index += 1
        }
      }
      if (!closed) {
        throw new IllegalArgumentException(s"Unclosed string in formula: $expression")
      }
      (builder.toString(), index)
    }
  }
}
