package com.ambiata.ivory.benchmark

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.reduction._
import com.google.caliper._
import org.joda.time.{Days => JodaDays, LocalDate => JodaLocalDate, LocalDateTime => JodaLocalDateTime, Seconds => JodaSeconds}

object ReducerBenchApp extends App {
  Runner.main(classOf[ReducerBench], args)
}

class ReducerBench extends SimpleScalaBenchmark {

  val intValues = (1 to 10000).toArray

  def time_sum(n: Int) = {
    val r = new SumReducer[Int]()
    repeat(n)(run(r, intValues))
  }

  def time_sum_notspec(n: Int) = {
    val r = new SumReducer[Int]()
    repeat(n)(runNotSpec(r, intValues))
  }

  def time_sum_inline(n: Int) =
    repeat(n)(intValues.sum)

  def time_mean(n: Int) = {
    val r = new MeanReducer[Int]()
    repeat(n)(run(r, intValues))
  }

  def time_mean_inline(n: Int) =
    repeat(n) {
      var count = 0
      var sum = 0
      intValues.foreach { i =>
        sum += i
        count += 1
      }
      sum.toDouble / count.toDouble
    }

  def runNotSpec[A, B, C](r: ReductionFold[A, B, C], l: Array[B]): C =
     run(r, l)

  def run[A, @specialized(Int, Float, Double, Boolean) B, C](r: ReductionFold[A, B, C], l: Array[B]): C = {
    var s = r.initial
    l.foreach { i =>
      s = r.fold(s, i)
    }
    r.aggregate(s)
  }

  val date = Date(2014, 10, 22)
  val dt = DateTime(2014, 10, 22, 2395)
  val dateJoda = new JodaLocalDate(date.year.toInt, date.month.toInt, date.day.toInt)
  val datetimeJoda = new JodaLocalDateTime(dt.date.year.toInt, dt.date.month.toInt, dt.date.day.toInt,
    dt.time.hours, dt.time.minuteOfHour, dt.time.secondOfMinute)
  val startDateJoda = new JodaLocalDate("2000-01-01")
  val startDateTimeJoda = new JodaLocalDateTime("2000-01-01")

  def time_date(n: Int) =
    repeat(n)(DateTimeUtil.toDays(date))

  def time_date_joda(n: Int) =
    repeat(n)(JodaDays.daysBetween(startDateJoda, dateJoda).getDays)

}
