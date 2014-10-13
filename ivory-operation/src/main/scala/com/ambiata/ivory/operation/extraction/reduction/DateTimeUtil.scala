package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._

/** Reimplementation of some of JodaTime's functionality for performance */
object DateTimeUtil {

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

  val monthSecondArray: Array[Int] = Array(
           0
         , 2678400
         , 5097600
         , 7776000
         , 10368000
         , 13046400
         , 15638400
         , 18316800
         , 20995200
         , 23587200
         , 26265600
         , 28857600
        )

  /* Calculate the seconds since the year 2000 */
  // Warning, doesn't do daylight savings
  def toSeconds(dt: DateTime): Long = {
    var ret  = (dt.underlying & 0xffffffff).toInt.toLong
    val date = dt.date
    val d = date.day
    val y = date.year
    val m = date.month

    if (m < 1 || m > 12)
      Crash.error(Crash.Invariant, s"Incorrect month for datetime $dt")

    ret += (d-1) * 86400 // 24 * 60 * 60
    // Add the days for each month past
    ret += monthSecondArray(m-1)

    // Add a day if it's a leap year and we're past Feb
    if (((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) && m > 2)
      ret += 86400

    // Add a day for each leap year since 2000, or subtract one for each before.
    if (y > 2000) {
      ret += (y-1997)/4 * 86400
      ret -= (y-2001)/100 * 86400
      ret += (y-2001)/400 * 86400
    } else {
      ret += (y-2000)/4 * 86400
      ret -= (y-2000)/100 * 86400
      ret += (y-2000)/400 * 86400      
    }

    ret += (y-2000).toLong * 31536000
    ret
  }

  /* Calculate the days since the year 2001-01-01, an alternative JodaTime version is included in the spec */
  def toDays(date: Date): Int = {
    val d = date.day
    val m = date.month
    val y = date.year

    if (m < 1 || m > 12)
      Crash.error(Crash.Invariant, s"Incorrect month for date $date")

    var ret = d - 1
    // Add the days for each month past
    ret += monthDayArray(m-1)

    // Add a day if it's a leap year and we're past Feb
    if (((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) && m > 2)
        ret += 1

    // Add a day for each leap year since 2000, or subtract one for each before.
    if (y > 2000) {
      ret += (y-1997)/4
      ret -= (y-2001)/100
      ret += (y-2001)/400
    } else {
      ret += (y-2000)/4
      ret -= (y-2000)/100
      ret += (y-2000)/400      
    }

    ret += (y-2000) * 365
    ret
  }
}
