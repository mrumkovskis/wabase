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
    s.length match {
      case 10 =>
        Format.xsdDate.parse(s)
      case 16 | 19 | 21 | 22 | 23 | 24 =>
        Format.xsdDate.parse(Format.xsdDate(Format.parseDateTime(s)))
      case _ =>
        throw new BusinessException(s"Unsupported date format - $s")
    }

  def parseDateTime(s: String) =
    s.length match {
      case 24 =>
        Format.jsIsoDateTime.parse(s)
      case 23 =>
        s.charAt(10) match {
          case ' ' => Format.humanDateTimeWithMillis.parse(s)
          case 'T' => Format.xlsxDateTime.parse(s)
          case  _  => Format.uriDateTimeWithMillis.parse(s)
        }
      case 22 =>
        s.charAt(10) match {
          case ' ' => Format.humanDateTimeWithMillis.parse(s"${s}0")
          case 'T' => Format.xlsxDateTime.parse(s"${s}0")
          case  _  => Format.uriDateTimeWithMillis.parse(s"${s}0")
        }
      case 21 =>
        s.charAt(10) match {
          case ' ' => Format.humanDateTimeWithMillis.parse(s"${s}00")
          case 'T' => Format.xlsxDateTime.parse(s"${s}00")
          case  _  => Format.uriDateTimeWithMillis.parse(s"${s}00")
        }
      case 19 =>
        s.charAt(10) match {
          case ' ' => Format.humanDateTime.parse(s)
          case 'T' => Format.timerDate.parse(s)
          case  _  => Format.uriDateTime.parse(s)
        }
      case 16 =>
        s.charAt(10) match {
          case ' ' => Format.humanDateTimeMin.parse(s)
          case 'T' => Format.timerDateHTML5.parse(s)
          case  _  => Format.uriDateTimeMin.parse(s)
        }
      case 10 =>
        Format.xsdDate.parse(s)
      case x if x > 10 =>
        val st = s.charAt(10) match {
          case ' ' => s"${s.substring(0, 10)}T${s.substring(11)}"
          case 'T' => s
          case  _  => s"${s.substring(0, 10)}T${s.substring(11)}"
        }
        try Date.from(Instant.parse(st)) catch {
          case ex: java.time.format.DateTimeParseException =>
            throw new BusinessException(s"Unsupported timestamp format. ${ex.getMessage}")
        }
      case _ =>
        throw new BusinessException(s"Unsupported timestamp format - $s")
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

  override def convertToType(value: Any, targetClass: Class[_]): Any = value match {
    case s: java.lang.String          => targetClass match {
      case ClassOfJavaSqlDate            => new java.sql.Date     (Format.parseDate(s)    .getTime)
      case ClassOfJavaSqlTimestamp       => new java.sql.Timestamp(Format.parseDateTime(s).getTime)
      case ClassOfJavaTimeLocalDate      => new java.sql.Date     (Format.parseDate(s)    .getTime).toLocalDate
      case ClassOfJavaTimeLocalDateTime  => new java.sql.Timestamp(Format.parseDateTime(s).getTime).toLocalDateTime
      case ClassOfJavaUtilDate           => Format.parseDateTime(s)
      case _                             => super.convertToType(value, targetClass)
    }
    case _: java.sql.Date                => super.convertToType(value, targetClass) // guard to avoid case java.util.Date below
    case _: java.sql.Time                => super.convertToType(value, targetClass) // guard to avoid case java.util.Date below
    case t: java.sql.Timestamp        => targetClass match {
      case ClassOfString                 => t.toString match {
                                              case s if s endsWith ".0" => s.substring(0, 19)
                                              case s => s
                                            }
      case _                             => super.convertToType(value, targetClass)
    }
    case t: java.time.LocalDateTime   => targetClass match {
      case ClassOfString                 => java.sql.Timestamp.valueOf(t).toString match {
                                              case s if s endsWith ".0" => s.substring(0, 19)
                                              case s => s
                                            }
      case _                             => super.convertToType(value, targetClass)
    }
    case t: java.time.LocalTime       => targetClass match {
      case ClassOfString                 => java.sql.Time.valueOf(t).toString
      case _                             => super.convertToType(value, targetClass)
    }
    case t: java.util.Date            => targetClass match {
      case ClassOfString                 => new java.sql.Timestamp(t.getTime).toString match {
                                              case s if s endsWith ".0" => s.substring(0, 19)
                                              case s => s
                                            }
      case _                             => super.convertToType(value, targetClass)
    }
    case _                            => super.convertToType(value, targetClass)
  }
}
