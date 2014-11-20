package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries._, Arbitraries._
import com.ambiata.ivory.core.gen.GenDate
import org.joda.time.{Days => JodaDays, LocalDate => JodaLocalDate, LocalDateTime => JodaLocalDateTime, Seconds => JodaSeconds}
import org.scalacheck.Arbitrary
import org.specs2.{ScalaCheck, Specification}

class DateTimeUtilSpec extends Specification with ScalaCheck { def is = s2"""
  Can calculate the number of days since the turn of the century correctly     $days
  Can calculate the date from a number of days since 1600-03-01                $dates

  Days converions are symmetric                                                $daysSymmetry
  Dates converions are symmetric                                               $datesSymmetry

  Can minus a number of days                                                   $minusDays
  Can minus a number of months                                                 $minusMonths
  Can minus a number of years                                                  $minusYears

"""

  def days = prop((d: Date) =>
    DateTimeUtil.toDays(d) ==== slowToD(d)
  )

  def dates = prop{ (d: Int) =>
    // Number of days between Date.minValue and Date.maxValue is 511644
    val days = Math.abs(d) % 511644
    DateTimeUtil.fromDays(days) ==== slowFromD(days)
  }

  def datesSymmetry = prop((d: Date) =>
    DateTimeUtil.fromDays(DateTimeUtil.toDays(d)) ==== d
  ).set(minTestsOk = 1000)

  def daysSymmetry = prop { (d: Int) =>
    val days = Math.abs(d) % 511644
    DateTimeUtil.toDays(DateTimeUtil.fromDays(days)) ==== days
  }

  def slowToD(d: Date): Int =
    JodaDays.daysBetween(new JodaLocalDate("1600-03-01"), new JodaLocalDate(d.year.toInt, d.month.toInt, d.day.toInt)).getDays

  def slowFromD(d: Int): Date =
    Date.fromLocalDate(new JodaLocalDate("1600-03-01").plusDays(d))

  def minusDays = prop { (d: Date, s: Short) =>
    val i = Math.abs(s)
    DateTimeUtil.minusDays(d, i) ==== Date.fromLocalDate(d.localDate.minusDays(i))
  }.set(minTestsOk = 1000)

  def minusMonths = prop { (d: Date, s: Short) =>
    val i = Math.abs(s)
    DateTimeUtil.minusMonths(d, i) ==== Date.fromLocalDate(d.localDate.minusMonths(i))
  }.set(minTestsOk = 1000)

  def minusYears = prop { (d: Date, s: Short) =>
    val i = Math.abs(s)
    DateTimeUtil.minusYears(d, i) ==== Date.fromLocalDate(d.localDate.minusYears(i))
  }.set(minTestsOk = 1000)
}
