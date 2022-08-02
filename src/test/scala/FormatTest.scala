package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class FormatTest extends FlatSpec with Matchers {
  import java.util.Calendar
  val cal = Calendar.getInstance
  cal.clear(Calendar.ZONE_OFFSET)
  cal.set(Calendar.YEAR, 2011)
  cal.set(Calendar.MONTH, 11)
  cal.set(Calendar.DATE, 12)
  cal.set(Calendar.HOUR_OF_DAY, 12)
  cal.set(Calendar.MINUTE, 12)
  cal.set(Calendar.SECOND, 59)
  cal.set(Calendar.MILLISECOND, 1)

  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12 12:12:59.001").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12_12:12:59.001").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12T12:12:59.001").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12 12:12:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12_12:12:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12T12:12:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12 12:12:59.1").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12_12:12:59.1").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-12T12:12:59.1").getTime)

  cal.set(Calendar.MILLISECOND, 0)
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-12 12:12:59").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-12_12:12:59").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-12T12:12:59").getTime

  cal.set(Calendar.SECOND, 0)
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-12 12:12").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-12_12:12").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-12T12:12").getTime

  cal.set(Calendar.MINUTE, 0)
  cal.set(Calendar.HOUR_OF_DAY, 0)
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12 00:00:00.0").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12_00:00:00.0").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12T00:00:00.0").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12 00:00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12_00:00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12T00:00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12 00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12_00:00").getTime
  cal.getTimeInMillis shouldBe Format.parseDate("2011-12-12T00:00").getTime
}
