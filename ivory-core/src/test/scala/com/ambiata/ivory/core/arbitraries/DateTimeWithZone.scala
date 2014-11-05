package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._
import org.joda.time.DateTimeZone


/** a datetime and its corresponding timezone */
case class DateTimeWithZone(datetime: DateTime, zone: DateTimeZone)

object DateTimeWithZone {
  implicit def DateTimeWithZoneArbitrary: Arbitrary[DateTimeWithZone] =
    Arbitrary(GenDate.dateTimeWithZone.map({ case (dt, z) => DateTimeWithZone(dt, z) }))
}
