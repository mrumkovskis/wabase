package org.wabase

import java.util.Locale
import java.time._
import java.time.chrono.Chronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, FormatStyle}
import java.time.temporal._

import scala.util.matching.Regex
import scala.language.postfixOps
import scala.util.Try
import scala.util.matching.Regex.Match

object Calendar {

  val timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss")

  class calendar extends calendar_base {
    var value: sDate = null
  }

  class calendar_time extends calendar_base {
    var value: Timestamp = null
  }

  class calendar_base {
    var locale: String = null
    var today: sDate = null
    var month: String = null
    var year: String = null
    var days: List[String] = Nil
    var weeks: List[String] = Nil
    var calendar: List[String] = Nil
  }

  def calendar(locale: String, value: String): List [calendar] = {
    val Array(lang, country) = locale.split("-").padTo(2, null)
    val loc = Option(country).map(new Locale(lang, _)).getOrElse(new Locale(lang))
    if (value == null || value.isEmpty) {
      val dto = new calendar
      List(calendar_entry(loc, LocalDate.now, dto))
    } else
      dateOptions(loc, value)
        .map { d =>
          val dto = new calendar
          dto.value = d
          calendar_entry(loc, d.toLocalDate, dto)
        }
  }

  def calendar_time(locale: String, value: String): List[calendar_time] = {
    val Array(lang, country) = locale.split("-").padTo(2, null)
    val loc = Option(country).map(new Locale(lang, _)).getOrElse(new Locale(lang))
    if (value == null || value.isEmpty) {
      val dto = new calendar_time
      List(calendar_entry(loc, LocalDate.now, dto))
    } else
      dateTimeOptions(loc, value)
        .map { t =>
          val dto = new calendar_time
          dto.value = t
          calendar_entry(loc, LocalDate.from(t.toLocalDateTime), dto)
        }
  }

  def calendar_entry(locale: Locale, date: LocalDate, dto: calendar_base): dto.type = {
    dto.locale = locale.toString
    dto.today = today
    dto.month = month(date, locale)
    dto.year = year(date)
    dto.days = days(locale)
    dto.weeks = weeks(date)
    dto.calendar = calendar(date, locale, None)
    dto
  }

  def today = {
    java.sql.Date.valueOf(LocalDate.now)
  }

  def month(date: LocalDate, locale: Locale) = {
    date.getMonth.getDisplayName(java.time.format.TextStyle.FULL_STANDALONE, locale)
  }

  def year(date: LocalDate) = {
    f"${date.getYear}%04d"
  }

  def days(locale: Locale) = {
    val firstDay = WeekFields.of(locale).getFirstDayOfWeek.getValue - 1
    val days = DayOfWeek.values.map(d => d.getValue -> d.getDisplayName(format.TextStyle.FULL_STANDALONE, locale)).toMap
    (0 to 6).map(i => (i + firstDay) % 7 + 1).map(days(_)).toList
  }

  def weeks(date: LocalDate) = {
    val wnr = date.withDayOfMonth(1).get(ChronoField.ALIGNED_WEEK_OF_YEAR)
    0 to 5 map (_ + wnr) map (_.toString) toList
  }

  def calendar(date: LocalDate, locale: Locale, time: Option[String]) = {
    val timeStr = time.map(" " + _).getOrElse("")
    val firstDayOfMonth = date.withDayOfMonth(date.range(ChronoField.DAY_OF_MONTH).getMinimum.toInt)
    val dayNr = firstDayOfMonth.getDayOfWeek.getValue
    val init = firstDayOfMonth //set date to nearest first day of week containing first day of month of this date
      .minusDays((7 - (WeekFields.of(locale).getFirstDayOfWeek.getValue - dayNr)) % 7)
    (1 until 42).scanLeft(init) { (d, _) => d.plusDays(1) }
      .map(_.toString + timeStr)
      .toList
  }

  def dateTimeOptions(locale: Locale, str: String): List[Timestamp] = {
    val timeOnlyRegex = """(?<h>\d{1,2})(?:(?:[:])(?<m>\d{1,2}))?(?:(?:[:])(?<s>\d{1,2}))?"""r
    val timeRegex =  """(?<h>\d{1,2})(?:(?:[:])(?<m>\d{1,2}))(?:(?:[:])(?<s>\d{1,2}))?"""r
    def timematch(m: Match, n: String) = Option(m.group(n)).map(_.toInt).getOrElse(0)
    def time(r: Regex) = r
      .findAllMatchIn(str)
      .map(m => (timematch(m, "h"), timematch(m, "m"), timematch(m, "s")))
      .toList
      .headOption
      .getOrElse((0, 0, 0))

    ("""\d{1,2}([:]\d{1,2}){1,2}|(^\s*\d{1,2}\s*$)""".r.replaceAllIn(str, "").trim match { //extract date part
      case x if x.isEmpty /* only time */ =>
        if (str.isEmpty) Nil else {
          val (h, m, s) = time(timeOnlyRegex)
          Try(LocalDateTime.now.withHour(h).withMinute(m).withSecond(s)).toOption.toList
        }
      case x =>
        val (h, m, s) = time(timeRegex)
        dateOptions(locale, x).flatMap(d => Try(d.toLocalDate.atTime(h, m, s)).toOption.toList)
    })
      .map(java.sql.Timestamp.valueOf)
  }


  def dateOptions(locale: Locale, str: String): List[sDate] = {
    val ymd_ord = ymd_orders(locale)
    val d = LocalDate.now
    val twoDigitYear = d.getYear % 100
    val roundedToCentury = d.getYear - twoDigitYear

    def normalizeYear(y: Int) =
      if (y < 100) if (y > twoDigitYear + 5) roundedToCentury - 100 + y else roundedToCentury + y else y


    ("""\d*""".r
      .findAllIn(str)
      .toList
      .filter(d => d.nonEmpty && d.length < 5)
      .map(_.toInt)
      .take(3)
      .toVector match {
      case Vector() => Nil
      case Vector(x) =>
        Try(d.withDayOfMonth(x)).toOption.toList
      case dm if dm.size == 2 =>
        val h = ymd_ord.head
        (if (h(1) < h(2)) List(Vector(0, 1), Vector(1, 0))
        else List(Vector(1, 0), Vector(0, 1)))
          .flatMap { case i =>
            Try(LocalDate.of(d.getYear, dm(i(0)), dm(i(1)))).toOption.toList
          }
          .distinct
      case ymd =>
        ymd_ord.flatMap { case i =>
          Try(LocalDate.of(normalizeYear(ymd(i(0))), ymd(i(1)), ymd(i(2)))).toOption.toList
        }
    }).map (java.sql.Date.valueOf).distinct
  }

  private def ymd_orders(locale: Locale) = {
    val format =
      DateTimeFormatterBuilder
        .getLocalizedDateTimePattern(FormatStyle.SHORT, FormatStyle.SHORT, Chronology.ofLocale(locale), locale)

    val m =
      List("y" -> Math.max(format.indexOf("y"), 0),
        "M" -> Math.max(format.indexOf("M"), 1),
        "d" -> Math.max(format.indexOf("d"), 2))
        .sortBy(_._2)
        .zipWithIndex
        .map { case ((x, _), i) => (x, i) }
        .toMap

    List(m("y"), m("M"), m("d"))
      .permutations
      .map (_.toVector)
      .toList
  }

}
