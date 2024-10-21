package org.wabase

import org.mojoz.querease.ValueConverter
import org.mojoz.querease.ValueConverter.{ClassOfJavaSqlDate, ClassOfJavaSqlTimestamp}
import org.mojoz.querease.ValueConverter.{ClassOfJavaTimeLocalDate, ClassOfJavaTimeLocalDateTime}
import org.mojoz.querease.ValueConverter.{ClassOfJavaUtilDate, ClassOfString}

import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date
import scala.util.control.NonFatal

object Format extends ValueConverter {

  class ThreadLocalDateFormat(val pattern: String) extends ThreadLocal[SimpleDateFormat] {
    override def initialValue = { val f = new SimpleDateFormat(pattern); f.setLenient(false); f }
    def apply(date: Date) = get.format(date)
    def format(date: Date) = get.format(date)
    def parse(str: String): Date =
      try get.parse(str)
      catch {
        case NonFatal(e) => throw new BusinessException(s"${e.getMessage}. Expected format - '$pattern'")
      }
  }

  def parseDate(s: String) =
    valueConverterDelegate.convertToType(s, ClassOfJavaUtilDate) match {
      case d: java.util.Date => d
      case _ => sys.error("Failed to parse date")
    }

  def parseDateTime(s: String) =
    valueConverterDelegate.convertToType(s, ClassOfJavaUtilDate) match {
      case d: java.util.Date => d
      case _ => sys.error("Failed to parse datetime")
    }

  val time                    = new ThreadLocalDateFormat("HH:mm")
  val date                    = new ThreadLocalDateFormat("dd.MM.yyyy")
  val dateLv                  = new ThreadLocalDateFormat("dd.MM.yyyy.")
  val dateTimeLv              = new ThreadLocalDateFormat("dd.MM.yyyy. HH:mm:ss")
  val xsdDate                 = new ThreadLocalDateFormat("yyyy-MM-dd")
  val couchDate               = new ThreadLocalDateFormat("yyyy.MM.dd_HH:mm:ss")
  val humanDateTime           = new ThreadLocalDateFormat("yyyy-MM-dd HH:mm:ss")
  val uriDateTime             = new ThreadLocalDateFormat("yyyy-MM-dd_HH:mm:ss")
  val timestamp               = new ThreadLocalDateFormat("yyyy.MM.dd HH:mm:ss.SSS")
  val humanDateTimeWithMillis = new ThreadLocalDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  val uriDateTimeWithMillis   = new ThreadLocalDateFormat("yyyy-MM-dd_HH:mm:ss.SSS")
  val humanDateTimeMin        = new ThreadLocalDateFormat("yyyy-MM-dd HH:mm")
  val uriDateTimeMin          = new ThreadLocalDateFormat("yyyy-MM-dd_HH:mm")
  val timerDateHTML5          = new ThreadLocalDateFormat("yyyy-MM-dd'T'HH:mm")
  val timerDate               = new ThreadLocalDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  val xlsxDateTime            = new ThreadLocalDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val jsIsoDateTime           = new ThreadLocalDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")

  def msgChain(e: Throwable, m: List[String] = Nil, separator: String = ", caused by: ", short: Boolean = false): String =
    msgList(e, m).mkString(separator)
  def msgList(e: Throwable, m: List[String] = Nil, short: Boolean = false): List[String] =
    if (e == null) m.reverse
    else {
      val msg = e match {
        case b: BusinessException => b.getMessage
        case e: Throwable => Some(e.getMessage).filter(_ != "" && short) getOrElse e.toString
      }
      if (Some(msg).filter(_ != null).exists(m.headOption.getOrElse("") endsWith _))
        msgList(e.getCause, m)
      else
        msgList(e.getCause, msg :: m)
    }

  def roundedIntervalCompact(interval: Long): String = roundedInterval(interval, true)
  def roundedInterval(interval: Long, compact: Boolean = false): String = {
    var x: Long = -1
    val ms = interval % 1000
    x = interval / 1000
    val seconds = x % 60
    x /= 60
    val minutes = x % 60
    x /= 60
    val hours = x % 24
    x /= 24
    val days = x
    val ft =
      if (compact) List(
        (days, "d", "d"),
        (hours, "h", "h"),
        (minutes, "m", "m"),
        (seconds, "s", "s"),
        (ms, "ms", "ms"))
      else List(
        (days, "day", "days"),
        (hours, "hour", "hours"),
        (minutes, "minute", "minutes"),
        (seconds, "second", "seconds"),
        (ms, "ms", "ms"))
    ft.dropWhile(x => x._1 == 0 && x._2 != "ms")
      .take(3)
      .map(x => "" + x._1 + " " + (if (x._1 == 1) x._2 else x._3))
      .mkString(" ")
  }

  def xmlEscape(s: String): String =
    s.replace("&", "&amp;")
     .replace("\"", "&quot;")
     .replace("\'", "&apos;")
     .replace("<", "&lt;")
     .replace(">", "&gt;")

  private lazy val valueConverterDelegate: ValueConverter =
    Option("app.value-converter").filter(config.hasPath).map(config.getString).map(c => getObjectOrNewInstance(c, "value converter")).map {
      case vc: ValueConverter => vc
      case x => sys.error(s"Expected type ValueConverter, got: ${x.getClass.getName}")
    }.getOrElse(new ValueConverter {})

  override def convertToType(value: Any, targetClass: Class[_]): Any =
    valueConverterDelegate.convertToType(value, targetClass)

  override def convertToString(value: Any): String =
    valueConverterDelegate.convertToString(value)
}
