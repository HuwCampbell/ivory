package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._

import org.joda.time.{DateTimeZone, Days => JodaDays}
import org.scalacheck._

import scala.collection.JavaConverters._
import scala.util.Try


object GenDate {
  def year: Gen[Short] =
    date.map(_.year)

  def month: Gen[Byte] =
    date.map(_.month)

  def day: Gen[Byte] =
    date.map(_.day)

  /** This date range is explicitly chosen to avoid out of range errors with joda,
      and to keep well with in the bounds of date to allow for neater offsets. */
  def date: Gen[Date] =
    dateIn(Date(1970, 1, 1), Date(2050, 12, 31))

  def dateIn(from: Date, to: Date): Gen[Date] =
    Gen.choose(0, JodaDays.daysBetween(from.localDate, to.localDate).getDays)
       .map(y => Date.fromLocalDate(from.localDate.plusDays(y)))

  def time: Gen[Time] = Gen.frequency(
    3 -> Gen.const(Time(0))
  , 1 -> Gen.choose(0, (60 * 60 * 24) - 1).map(Time.unsafe)
  )

  def dateTime: Gen[DateTime] = for {
    d <- date
    t <- time
  } yield d.addTime(t)

  def zone: Gen[DateTimeZone] =
    Gen.oneOf(DateTimeZone.getAvailableIDs.asScala.toSeq).map(DateTimeZone.forID)

  def dateTimeWithZone: Gen[(DateTime, DateTimeZone)] = for {
    dt <- dateTime
    z <- zone
    // There are issues with this specific timezone before 1980
    skip = z == DateTimeZone.forID("Africa/Monrovia") && dt.date.year <= 1980
    r <- if (skip || Try(dt.joda(z)).toOption.isEmpty) dateTimeWithZone else Gen.const(dt -> z)
  } yield r
}
