package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import org.scalacheck._, Arbitrary._
import org.joda.time.DateTimeZone
import Arbitraries._

/** a datetime which can not be parsed with the corresponding timezone */
case class BadDateTime(datetime: DateTime, zone: DateTimeZone)

object BadDateTime {
  // FIX ARB This nees some polish.
  implicit def BadDateTimeArbitrary: Arbitrary[BadDateTime] =
    Arbitrary(for {
      dt <- arbitrary[DateTime]
      opt <- arbitrary[DateTimeZone].map(z => Dates.dst(dt.date.year, z).flatMap({ case (firstDst, secondDst) =>
        val unsafeFirst = unsafeAddSecond(firstDst)
        val unsafeSecond = unsafeAddSecond(secondDst)
        try {
          unsafeFirst.joda(z)
          try {
            unsafeSecond.joda(z); None
          } catch {
            case e: java.lang.IllegalArgumentException => Some((unsafeSecond, z))
          }
        } catch {
          case e: java.lang.IllegalArgumentException => Some((unsafeFirst, z))
        }
      })).retryUntil(_.isDefined)
      (bad, z) = opt.get
    } yield BadDateTime(bad, z))

  def unsafeAddSecond(dt: DateTime): DateTime = {
    val (d1, h1, m1, s1) = (dt.date.day.toInt, dt.time.hours, dt.time.minuteOfHour, dt.time.secondOfMinute) match {
      case (d, 23, 59, 59) => (d + 1, 0, 0, 0)
      case (d, h, 59, 59)  => (d, h + 1, 0, 0)
      case (d, h, m, 59)   => (d, h, m + 1, 0)
      case (d, h, m, s)    => (d, h, m, s + 1)
    }
    DateTime.unsafe(dt.date.year, dt.date.month, d1.toByte, (h1 * 60 * 60) + (m1 * 60) + s1)
  }
}
