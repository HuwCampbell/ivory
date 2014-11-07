package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries._, Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._
import org.joda.time._, format.DateTimeFormat
import scalaz._, Scalaz._
import argonaut._, Argonaut._

class DatesSpec extends Specification with ScalaCheck { def is = s2"""

Date Parsing
------------

  Symmetric                                       $datesymmetric
  Invalid year                                    $year
  Invalid month                                   $month
  Invalid day                                     $day
  Edge cases                                      $edge
  Exceptional - non numeric values                $exceptional
  Round-trip with joda                            $joda
  Parses same as joda                             $jodaparse
  Encode/Decode Json are symmetric                $encodeDecodeJson
  Decode with bad year fails                      $encodeBadYear
  Decode with bad month fails                     $encodeBadMonth
  Decode with bad day fails                       $encodeBadDay

Date Time Parsing
-----------------

  Symmteric                                       $timesymmetric

Date Time Parsing w/ Zones
--------------------------

  Symmteric                                       $zonesymmetric

Generic Time Format Parsing
---------------------------

  Dates are recognized                            $parsedate
  Date/Times are recognized                       $parsetime
  Date/Time/Zones are recognized                  $parsezone
  Everything else fails                           $parsefail

"""
  case class GoodYear(y: Short)
  implicit def GoodYearArbitrary: Arbitrary[GoodYear] =
    Arbitrary(Gen.choose(1000, Short.MaxValue).map(i => GoodYear(i.toShort)))

  case class GoodMonth(m: Byte)
  implicit def GoodMonthArbitrary: Arbitrary[GoodMonth] =
    Arbitrary(Gen.choose(1, 12).map(i => GoodMonth(i.toByte)))

  // Doesnt check the edge cases, just ranges to the really safe values,
  // [1, 28]
  case class GoodDay(d: Byte)
  implicit def GoodDayArbitrary: Arbitrary[GoodDay] =
    Arbitrary(Gen.choose(1, 28).map(i => GoodDay(i.toByte)))


  case class BadYear(y: Short)
  implicit def BadYearArbitrary: Arbitrary[BadYear] =
    Arbitrary(Gen.choose(0, 999).map(i => BadYear(i.toShort)))

  case class BadMonth(m: Byte)
  implicit def BadMonthArbitrary: Arbitrary[BadMonth] =
    Arbitrary(Gen.choose(13, Byte.MaxValue).map(i => BadMonth(i.toByte)))

  // Doesnt check all the bad month/day/leapyear combos, just > 31
  case class BadDay(d: Byte)
  implicit def BadDayArbitrary: Arbitrary[BadDay] =
    Arbitrary(Gen.choose(32, Byte.MaxValue).map(i => BadDay(i.toByte)))

  def datesymmetric = prop((d: Date) =>
    Dates.date(d.hyphenated) must beSome(d))

  def year = prop((d: Date) =>
    Dates.date("0100-%02d-%02d".format(d.month, d.day)) must beNone)

  def month = prop((d: Date) =>
    Dates.date("%4d-13-%02d".format(d.year, d.day)) must beNone)

  def day = prop((d: Date) =>
    Dates.date("%4d-%02d-32".format(d.year, d.month)) must beNone)

  def exceptional = prop((d: Date) =>
    Dates.date(d.hyphenated.replaceAll("""\d""", "x")) must beNone)

  def joda = prop((d: Date) =>
    Dates.date(new LocalDate(d.year, d.month, d.day).toString("yyyy-MM-dd")) must beSome(d))

  def jodaparse = prop((d: Date) => {
    val j = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate(d.hyphenated)
    (j.getYear, j.getMonthOfYear, j.getDayOfMonth) must_== ((d.year.toInt, d.month.toInt, d.day.toInt)) })

  def encodeDecodeJson = prop((d: Date) =>
    Parse.decodeEither[Date](d.asJson.nospaces) must_== d.right)

  def encodeBadYear = prop((y: BadYear, m: GoodMonth, d: GoodDay) =>
    Parse.decodeEither[Date](Date.unsafeYmdToInt(y.y, m.m, d.d).asJson.nospaces) must_== "Date: []".left)

  def encodeBadMonth = prop((y: GoodYear, m: BadMonth, d: GoodDay) =>
    Parse.decodeEither[Date](Date.unsafeYmdToInt(y.y, m.m, d.d).asJson.nospaces) must_== "Date: []".left)

  def encodeBadDay = prop((y: GoodYear, m: GoodMonth, d: BadDay) =>
    Parse.decodeEither[Date](Date.unsafeYmdToInt(y.y, m.m, d.d).asJson.nospaces) must_== "Date: []".left)

  def edge = {
    (Dates.date("2000-02-29") must beSome(Date(2000, 2, 29))) and
    (Dates.date("2001-02-29") must beNone)
  }

  def zonesymmetric = prop((dz: DateTimeWithZone, ivory: DateTimeZone) => runExample(dz.datetime, dz.zone, ivory) ==> {
    val d = dz.datetime
    val local = dz.zone
    (Dates.datetimezone(d.iso8601(local), local) must beSome(d)) and
    (Dates.datetimezone(d.iso8601(local), ivory) must beSome((iDate: DateTime) => {
      // TODO: Fix when we handle DST
      val ijd = d.joda(local).withZone(ivory)
      val jdt = if(ijd.withEarlierOffsetAtOverlap == ijd)
        iDate.joda(ivory).withEarlierOffsetAtOverlap.withZone(local)
      else
        iDate.joda(ivory).withLaterOffsetAtOverlap.withZone(local)
      DateTime.fromJoda(jdt) must_== d
    }))
  }).set(minTestsOk = 10000)

  def timesymmetric = prop((dz: DateTimeWithZone, ivory: DateTimeZone) => runExample(dz.datetime, dz.zone, ivory) ==> {
    val d = dz.datetime
    val local = dz.zone
    (Dates.datetime(d.localIso8601, local, local) must beSome(d)) and
    (Dates.datetime(d.localIso8601, local, ivory) must beSome((iDate: DateTime) => {
      // TODO: Fix when we handle DST
      val ijd = d.joda(local).withZone(ivory)
      val jdt = if(ijd.withEarlierOffsetAtOverlap == ijd)
        iDate.joda(ivory).withEarlierOffsetAtOverlap.withZone(local)
      else
        iDate.joda(ivory).withLaterOffsetAtOverlap.withZone(local)
      DateTime.fromJoda(jdt) must_== d
    }))
  }).set(minTestsOk = 10000)

  def runExample(d: DateTime, local: DateTimeZone, ivory: DateTimeZone): Boolean =
    (local.toString != "Africa/Monrovia" || d.date.year > 1980) // for some reason there are issues with this specific timezone before 1980

  def parsedate = prop((dtz: DateTimeWithZone) =>
    Dates.parse(dtz.datetime.date.hyphenated, dtz.zone, dtz.zone) must beSome((nd: Date \/ DateTime) => nd.toEither must beLeft(dtz.datetime.date))
  )

  def parsetime = prop((dtz: DateTimeWithZone) =>
    Dates.parse(dtz.datetime.localIso8601, dtz.zone, dtz.zone) must beSome((nd: Date \/ DateTime) => nd.toEither must beRight(dtz.datetime))
  )

  def parsezone = prop((dtz: DateTimeWithZone) =>
    Dates.parse(dtz.datetime.iso8601(dtz.zone), dtz.zone, dtz.zone) must beSome((nd: Date \/ DateTime) => nd.toEither must beRight(dtz.datetime))
  )

  def parsefail =
    (Dates.parse("2001-02-29", DateTimeZone.UTC, DateTimeZone.UTC) must beNone) and
    (Dates.parse("2001-02-20T25:10:01", DateTimeZone.UTC, DateTimeZone.UTC) must beNone) and
    (Dates.parse("2001-02-20T20:10:01-24:00", DateTimeZone.UTC, DateTimeZone.UTC) must beNone) and
    prop((bad: BadDateTime) => Dates.parse(bad.datetime.localIso8601, bad.zone, DateTimeZone.UTC) must beNone)

}
