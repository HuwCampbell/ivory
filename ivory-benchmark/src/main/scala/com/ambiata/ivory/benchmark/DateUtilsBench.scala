package com.ambiata.ivory.benchmark

import java.lang.System.identityHashCode

import com.ambiata.ivory.core._
import com.google.caliper._

object DateUtilsBenchApp extends App {
  Runner.main(classOf[DateUtilsBench], args)
}

class DateUtilsBench extends SimpleScalaBenchmark {

  val start = Date(2001, 1, 1)
  val end = Date(2050, 12, 31)
  val dates = (start.int to end.int).flatMap(Date.fromInt).toList
  val yodaDates = dates.map(_.localDate)

  def time_minusDays(n: Int) =
    repeat(n) {
      var x = 0
      dates.foreach {
        date => x += DateTimeUtil.minusDays(date, 2130).underlying
      }
      x
    }

  def time_minusDaysOld(n: Int) =
    repeat(n) {
      var x = 0
      dates.foreach {
        date => x += minusDaysOld(date, 2130).underlying
      }
      x
    }


  def time_minusDays_joda(n: Int) =
    repeat(n) {
      var x = 0
      yodaDates.foreach {
        date => x += identityHashCode(date.minusDays(2130))
      }
      x
    }

  def time_minusMonths(n: Int) =
    repeat(n) {
      var x = 0
      dates.foreach {
        date => x += DateTimeUtil.minusMonths(date, 25).underlying
      }
      x
    }

  def time_minusMonths_joda(n: Int) =
    repeat(n) {
      var x = 0
      yodaDates.foreach {
        date => x += identityHashCode(date.minusMonths(25))
      }
      x
    }

  def time_minusYears(n: Int) =
    repeat(n) {
      var x = 0
      dates.foreach {
        date => x += DateTimeUtil.minusYears(date, 11).underlying
      }
      x
    }

  def time_minusYears_joda(n: Int) =
    repeat(n) {
      var x = 0
      yodaDates.foreach {
        date => x += identityHashCode(date.minusYears(11))
      }
      x
    }

  val monthDayArray: Array[Int] = Array(
           0
         , 31
         , 59
         , 90
         , 120
         , 151
         , 181
         , 212
         , 243
         , 273
         , 304
         , 334
        )


  def minusDaysOld(date: Date, offset: Int): Date = {

    def daysInYear(y: Short, m: Int): Int =
      monthDayArray(m - 1) + (if (m > 2 && DateTimeUtil.isLeapYear(y)) 1 else 0)
    def daysInMonth(y: Short, m: Int): Int =
      DateTimeUtil.monthLengths(m - 1) + (if (m == 2 && DateTimeUtil.isLeapYear(y)) 1 else 0)

    var y = date.year
    var m = date.month.toInt
    val d = date.day

    // The special case where we're still in the same month
    val d2 = if (offset < d) {
      d - offset
    } else {
      var daysLeft = offset - d

      var currentDays = daysInYear(y, m)
      while (currentDays <= daysLeft) {
        daysLeft -= currentDays
        y = (y - 1).toShort
        currentDays = if (DateTimeUtil.isLeapYear(y)) 366 else 365
        // Start from December, but not 12 because we always decrement one below
        m = 13
      }

      // We're always going back _at least_ one month at this point
      m -= 1

      // Loop back through the months of this year until we run out of days
      currentDays = daysInMonth(y, m)
      while (currentDays <= daysLeft) {
        daysLeft -= currentDays
        m -= 1
        currentDays = daysInMonth(y, m)
      }
      currentDays - daysLeft
    }
    Date.unsafe(y, m.toByte, d2.toByte)
  }


}
