package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._, Arbitraries._
import org.joda.time.{Days => JodaDays, LocalDate => JodaLocalDate, LocalDateTime => JodaLocalDateTime, Seconds => JodaSeconds}
import org.scalacheck.Arbitrary
import org.specs2.{ScalaCheck, Specification}

class DateTimeUtilSpec extends Specification with ScalaCheck { def is = s2"""
  Can calculate the number of days since the turn of the century correctly     $days
  Seconds do not overflow at the year 3000                                     $overflow
  Can calculate the number of seconds since the turn of the century correctly  $seconds
"""
	
  case class TestDate(d: Date)

  /* The normal arbitrary goes to year 3000, which overflows the JodaTime test version (but not the actual version) */
  implicit def TestDateArbitrary: Arbitrary[TestDate] =
    Arbitrary(genDate(Date(1950, 1, 1), Date(2050, 12, 31)).map(TestDate.apply))

  def days = prop((d: Date) =>
    DateTimeUtil.toDays(d) ==== slowToD(d)
  )

  def overflow =
    DateTimeUtil.toSeconds(DateTime(3000, 12, 31, 7200)) ==== 31588452000L

  def seconds = prop((d: TestDate, t: Time) => {
  	val q = d.d.addTime(t)
    DateTimeUtil.toSeconds(q) ==== slowToS(q)
  })

  def slowToD(d: Date): Int =
    JodaDays.daysBetween(new JodaLocalDate("2000-01-01"), new JodaLocalDate(d.year.toInt, d.month.toInt, d.day.toInt)).getDays

  def slowToS(dt: DateTime): Long =
    JodaSeconds.secondsBetween(new JodaLocalDateTime("2000-01-01"), new JodaLocalDateTime(dt.date.year.toInt, dt.date.month.toInt,
      dt.date.day.toInt, dt.time.hours, dt.time.minuteOfHour, dt.time.secondOfMinute)).getSeconds
}
