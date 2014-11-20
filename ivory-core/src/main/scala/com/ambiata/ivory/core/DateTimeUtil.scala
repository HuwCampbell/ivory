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

  /* Calculate the days since the year 1600-03-01, an alternative JodaTime version is included in the spec */
  def toDays(date: Date): Int = {
    var d = date.day.toInt
    var m = date.month.toInt
    var y = date.year.toInt - 1600

    m = (m + 9) % 12
    y = y - m/10

    365*y + y/4 - y/100 + y/400 + (m*306 + 5)/10 + ( d - 1 )
  }

  /* Calculate the date from a number of days since 1600-03-01 */
  def fromDays(g: Int): Date = {
    var y = ((10000L*g + 14780)/3652425).toInt
    var ddd = g - (365*y + y/4 - y/100 + y/400)
    if (ddd < 0) {
      y = y - 1
      ddd = g - (365*y + y/4 - y/100 + y/400)
    }
    var mi = (100*ddd + 52)/3060
    var mm = (mi + 2)%12 + 1
    y = y + (mi + 2)/12
    var dd = ddd - (mi*306 + 5)/10 + 1
    Date.unsafe((y + 1600).toShort, mm.toByte, dd.toByte)
  }

  def isLeapYear(y: Short): Boolean =
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0

  def minusDays(date: Date, offset: Int): Date = {
    fromDays(toDays(date) - offset)
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
