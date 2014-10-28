package com.ambiata.ivory.core
package arbitraries

import org.scalacheck._, Arbitrary._
import org.joda.time.{DateTimeZone, Days => JodaDays}
import scala.collection.JavaConverters._
import Gen._

import scalaz.{Name => _, Value => _}

trait ArbitraryValues {

  implicit def EntityArbitrary: Arbitrary[Entity] =
    Arbitrary(Gen.identifier map Entity.apply)

  implicit def ValueArbitrary: Arbitrary[Value] =
    Arbitrary(Gen.frequency(
      1 -> (Gen.identifier map StringValue.apply)
      , 2 -> (arbitrary[Int] map IntValue.apply)
      , 2 -> (arbitrary[Long] map LongValue.apply)
      , 2 -> (arbitrary[Double] map DoubleValue.apply)
      , 2 -> (arbitrary[Boolean] map BooleanValue.apply)
      , 2 -> (arbitrary[Date] map DateValue.apply)
    ))

  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(Gen.choose(Priority.Min.toShort, Priority.Max.toShort).map(Priority.unsafe))

  def genDate(from: Date, to: Date): Gen[Date] =
    Gen.choose(0, JodaDays.daysBetween(from.localDate, to.localDate).getDays)
      .map(y => Date.fromLocalDate(from.localDate.plusDays(y)))

  implicit def DateArbitrary: Arbitrary[Date] =
    Arbitrary(genDate(Date(1970, 1, 1), Date(3000, 12, 31)))

  implicit def TwoDifferentDatesArbitrary: Arbitrary[UniqueDates] =
    Arbitrary(for {
      d1 <- genDate(Date(1970, 1, 1), Date(2100, 12, 31))
      d2 <- Gen.frequency(5 -> Date.minValue, 95 -> Gen.choose(1, 100).map(o => Date.fromLocalDate(d1.localDate.minusDays(o))))
      d3 <- Gen.frequency(5 -> Date.maxValue, 95 -> Gen.choose(1, 100).map(o => Date.fromLocalDate(d1.localDate.plusDays(o))))
    } yield UniqueDates(d2, d1, d3))

  /* Generate a distinct list of Dates up to size n */
  def genDates(nDates: Gen[Int]): Gen[List[Date]] =
    nDates.flatMap(n => Gen.listOfN(n, arbitrary[Date])).map(_.distinct)

  implicit def TimeArbitrary: Arbitrary[Time] =
    Arbitrary(Gen.frequency(
      3 -> Gen.const(Time(0))
      , 1 -> Gen.choose(0, (60 * 60 * 24) - 1).map(Time.unsafe)
    ))

  implicit def DateTimeArbitrary: Arbitrary[DateTime] =
    Arbitrary(for {
      d <- arbitrary[Date]
      t <- arbitrary[Time]
    } yield d.addTime(t))

  implicit def DateTimeWithZoneArbitrary: Arbitrary[DateTimeWithZone] =
    Arbitrary(for {
      dt <- arbitrary[DateTime]
      z <- arbitrary[DateTimeZone].retryUntil(z => try {
        dt.joda(z); true
      } catch {
        case e: java.lang.IllegalArgumentException => false
      })
    } yield DateTimeWithZone(dt, z))

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

  implicit def DateTimeZoneArbitrary: Arbitrary[DateTimeZone] = Arbitrary(for {
    zid <- Gen.oneOf(DateTimeZone.getAvailableIDs.asScala.toSeq)
  } yield DateTimeZone.forID(zid))

  implicit def NameArbitrary: Arbitrary[Name] = Arbitrary(
    for {
      firstCharacter <- frequency(4 -> const('-'), 96 -> alphaNumChar)
      otherCharacters <- GenPlus.nonEmptyListOf(frequency(2 -> const('_'), 2 -> const('-'), 96 -> alphaNumChar))
    } yield Name.reviewed((firstCharacter +: otherCharacters).mkString)
  )

  implicit def BadNameStringArbitrary: Arbitrary[BadNameString] = Arbitrary {
    oneOf("", "_name", "name1/name2", "name㭊").map(BadNameString)
  }

  implicit def GoodNameStringArbitrary: Arbitrary[GoodNameString] = Arbitrary {
    NameArbitrary.arbitrary.map(n => GoodNameString(n.name))
  }

  implicit def RandomNameStringArbitrary: Arbitrary[RandomNameString] = Arbitrary {
    frequency((50, BadNameStringArbitrary.arbitrary.map(_.name)), (50, GoodNameStringArbitrary.arbitrary.map(_.name)))
      .map(RandomNameString)
  }

}

object ArbitraryValues extends ArbitraryValues

case class Entity(value: String)

/** set of 3 unique dates so that earlier < now < later */
case class UniqueDates(earlier: Date, now: Date, later: Date)

/** a datetime and its corresponding timezone */
case class DateTimeWithZone(datetime: DateTime, zone: DateTimeZone)

/** a datetime which can not be parsed with the corresponding timezone */
case class BadDateTime(datetime: DateTime, zone: DateTimeZone)

/** good and bad name (which can or can't become Name instances) */
case class GoodNameString(name: String)
case class BadNameString(name: String)
case class RandomNameString(name: String)
