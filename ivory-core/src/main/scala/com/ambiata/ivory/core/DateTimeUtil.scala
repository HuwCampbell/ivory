package com.ambiata.ivory.core

/** Reimplementation of some of JodaTime's functionality for performance */
object DateTimeUtil {

  val monthLengths: Array[Byte] = Array(
      31
    , 28
    , 31
    , 30
    , 31
    , 30
    , 31
    , 31
    , 30
    , 31
    , 30
    , 31
  )

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
    if (m > 2 && isLeapYear(y))
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
    if (m > 2 && isLeapYear(y))
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

  def isLeapYear(y: Short): Boolean =
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0

  def minusDays(date: Date, offset: Int): Date = {

    def daysInYear(y: Short, m: Int): Int =
      monthDayArray(m - 1) + (if (m > 2 && isLeapYear(y)) 1 else 0)
    def daysInMonth(y: Short, m: Int): Int =
      monthLengths(m - 1) + (if (m == 2 && isLeapYear(y)) 1 else 0)

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
        currentDays = if (isLeapYear(y)) 366 else 365
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

  def minusMonths(date: Date, offset: Int): Date = {
    val y = date.year
    val m = date.month
    val thisYear = offset < m
    val y2 = if (thisYear) y           else (y - ((12 + offset - m) / 12)).toShort
    val m2 = (if (thisYear) m - offset else 12 + ((m - offset) % 12)).toByte
    unsafeDateRoundedDownToLastDayOfMonth(y2, m2, date.day)
  }

  def minusYears(date: Date, offset: Int): Date =
    unsafeDateRoundedDownToLastDayOfMonth((date.year - offset).toShort, date.month, date.day)

  /**
   * Ensure that the created dated will round down to the first date according to that month (and year).
   *
   * {{{
   *    Start             | End
   *    =====================================
   *    Date(2003, 2, 29) | Date(2003, 2, 28)
   *    Date(2003, 4, 31) | Date(2003, 4, 30)
   * }}}
   */
  def unsafeDateRoundedDownToLastDayOfMonth(y: Short, m: Byte, d: Byte): Date = {
    val d2: Byte =
      if (m == 2 && d > 28 && isLeapYear(y)) 29
      else {
        val maxMonth = monthLengths(m - 1)
        if (d < maxMonth) d else maxMonth
      }
    Date.unsafe(y, m, d2)
  }
}
