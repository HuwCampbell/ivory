package com.ambiata.ivory.benchmark

import com.ambiata.ivory.core._
import com.google.caliper._
import org.joda.time._, format.DateTimeFormat
import scalaz._, Scalaz._

object DatesBenchApp extends App {
  Runner.main(classOf[DatesBench], args)
}

/*
 * This is a (crude) experiment, demonstrating the performance of some of the more hideous
 * machanisms for date parsing. The goal of this is to try get more speed out of the ingestion
 * date parsing, where the naive joda implementation is not good enough.
 */
case class DatesBench() extends SimpleScalaBenchmark {
  /* ------ The Benchmarks ------ */

  def time_joda_ok(n: Int) =
    joda(n, "2012-01-01")

  def time_joda_invalid(n: Int) =
    joda(n, "2012-99-01")

  def time_joda_bad(n: Int) =
    joda(n, "201x-01-01")

  def time_regex_ok(n: Int) =
    regex(n, "2012-01-01")

  def time_regex_invalid(n: Int) =
    regex(n, "2012-99-01")

  def time_regex_bad(n: Int) =
    regex(n, "201x-01-01")

  def time_hand_ok(n: Int) =
    hand(n, "2012-01-01")

  def time_hand_invalid(n: Int) =
    hand(n, "2012-99-01")

  def time_hand_bad(n: Int) =
    hand(n, "201x-01-01")

  def time_hand_less_ok(n: Int) =
    hand_less_alloc(n, "2012-01-01")

  def time_hand_less_invalid(n: Int) =
    hand_less_alloc(n, "2012-99-01")

  def time_hand_less_bad(n: Int) =
    hand_less_alloc(n, "201x-01-01")

  /*
   * The basic joda approach, note: parseLocalDate throws exceptions so the catch is required.
   */
  def joda(n: Int, s: String) =
    repeat[String \/ Date](n) {
      try {
        val d = DateTimeFormat.forPattern("yyyy-MM-dd").parseLocalDate(s)
        Date.unsafe(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte).right[String]
      } catch { case e: Throwable => "bad".left }
    }

  /*
   * A dumb regex approach, just to elliminate this idea from anyone stubling across the code.
   */
  def regex(n: Int, s: String) = {
    val DateParser = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
    repeat[String \/ Date](n) {
       s match {
         case DateParser(y, m, d) =>
           try Date.create(y.toShort, m.toByte, d.toByte).get.right
           catch { case e: Throwable => "bad".left }
         case _ => "bad".left
       }
    }
  }

  /*
   * A crude parser that unpacks things by hand.
   */
  def hand(n: Int, s: String) =
    repeat[String \/ Date](n) {
       if (s.length != 10 || s.charAt(4) != '-' || s.charAt(7) != '-')
         "bad".left
       else try
         Date.create(s.substring(0, 4).toShort, s.substring(5, 7).toByte, s.substring(9, 10).toByte).get.right
       catch { case e: Throwable => "bad".left }
    }

  /*
   * A crude parser that unpacks things by hand with less allocation
   */
  def hand_less_alloc(n: Int, s: String) =
    repeat[String \/ Date](n) {
       if (s.length != 10 || s.charAt(4) != '-' || s.charAt(7) != '-')
         "bad".left
       else try {
         val y = s.substring(0, 4).toShort
         val m = s.substring(5, 7).toByte
         val d = s.substring(9, 10).toByte
         if (Date.isValid(y, m, d)) Date.unsafe(y, m, d).right else "bad".left
       } catch { case e: Throwable => "bad".left }
    }
}
