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
        date => x += DateTimeUtil.minusDays(date, 450).underlying
      }
      x
    }

  def time_minusDays_joda(n: Int) =
    repeat(n) {
      var x = 0
      yodaDates.foreach {
        date => x += identityHashCode(date.minusDays(450))
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
}
