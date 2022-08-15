package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class FormatTest extends FlatSpec with Matchers {
  import java.util.Calendar
  val cal = Calendar.getInstance
  cal.clear(Calendar.ZONE_OFFSET)
  cal.set(Calendar.YEAR, 2011)
  cal.set(Calendar.MONTH, 11)
  cal.set(Calendar.DATE, 13)
  cal.set(Calendar.HOUR_OF_DAY, 14)
  cal.set(Calendar.MINUTE, 15)
  cal.set(Calendar.SECOND, 59)
  cal.set(Calendar.MILLISECOND, 1)

  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.001").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.001").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.001").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.1").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.1").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.1").getTime)

  cal.set(Calendar.MILLISECOND, 0)
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13 14:15:59").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13_14:15:59").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13T14:15:59").getTime

  cal.set(Calendar.SECOND, 0)
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13 14:15").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13_14:15").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13T14:15").getTime

  cal.set(Calendar.MINUTE, 0)
  cal.set(Calendar.HOUR_OF_DAY, 0)
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13 00:00:00.0").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13_00:00:00.0").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13T00:00:00.0").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13 00:00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13_00:00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13T00:00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13 00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13_00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13T00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13T00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-13").getTime
}
