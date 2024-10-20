package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers


class FormatTest extends FlatSpec with Matchers {
  import java.util.Calendar
  val zoneOffsetMillis = +3 * 60 * 60 * 1000
  val cal = Calendar.getInstance
  cal.set(Calendar.ZONE_OFFSET, zoneOffsetMillis)
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

  cal.set(Calendar.MILLISECOND, 10)
  cal.set(Calendar.ZONE_OFFSET, zoneOffsetMillis)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.010").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.010").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.010").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.01").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.01").getTime)

  cal.set(Calendar.MILLISECOND, 100)
  cal.set(Calendar.ZONE_OFFSET, zoneOffsetMillis)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.100").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.100").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.100").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.10").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.10").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.10").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13 14:15:59.1").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13_14:15:59.1").getTime)
  cal.getTimeInMillis should be (Format.parseDateTime("2011-12-13T14:15:59.1").getTime)

  cal.set(Calendar.MILLISECOND, 0)
  cal.set(Calendar.ZONE_OFFSET, zoneOffsetMillis)
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13 14:15:59").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13_14:15:59").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13T14:15:59").getTime

  cal.set(Calendar.SECOND, 0)
  cal.set(Calendar.ZONE_OFFSET, zoneOffsetMillis)
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13 14:15").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13_14:15").getTime
  cal.getTimeInMillis shouldBe Format.parseDateTime("2011-12-13T14:15").getTime

  cal.set(Calendar.MINUTE, 0)
  cal.set(Calendar.HOUR_OF_DAY, 0)
  cal.set(Calendar.ZONE_OFFSET, zoneOffsetMillis)
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

  cal.set(Calendar.YEAR, 1970)
  cal.set(Calendar.MONTH, 0)
  cal.set(Calendar.DATE, 1)
  cal.set(Calendar.HOUR_OF_DAY, 10)
  cal.set(Calendar.MINUTE, 11)
  cal.getTimeInMillis shouldBe Format.time.parse("10:11").getTime
  cal.set(Calendar.HOUR_OF_DAY, 15)
  cal.getTimeInMillis shouldBe Format.time.parse("15:11").getTime
}
