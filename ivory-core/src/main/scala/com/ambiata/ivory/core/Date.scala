package com.ambiata.ivory.core

import scalaz._, Scalaz._
import argonaut._, Argonaut._
import org.joda.time.LocalDate
import com.ambiata.mundane.parse.ListParser

/** a packed int | 16 bits: year represented as a short | 8 bits: month represented as a byte | 8 bits: day represented as a byte | */
class Date private(val underlying: Int) extends AnyVal {
  def components: (Short, Byte, Byte) =
    (year, month, day)

  def year: Short =
    (underlying >>> 16).toShort

  def month: Byte =
    (underlying >>> 8 & 0xff).toByte

  def day: Byte =
    (underlying & 0xff).toByte

  def int: Int =
    underlying

  def localDate: LocalDate =
    new LocalDate(year.toInt, month.toInt, day.toInt)

  def hyphenated: String =
    string("-")

  def slashed: String =
    string("/")

  def string(delim: String): String =
    s"%4d${delim}%02d${delim}%02d".format(year, month, day)

  def <(other: Date): Boolean =
    isBefore(other)

  def isBefore(other: Date): Boolean =
    int < other.int

  def <=(other: Date): Boolean =
    isBeforeOrEqual(other)

  def isBeforeOrEqual(other: Date): Boolean =
    int <= other.int

  def >(other: Date): Boolean =
    isAfter(other)

  def isAfter(other: Date): Boolean =
    int > other.int

  def >=(other: Date): Boolean =
    isAfterOrEqual(other)

  def isAfterOrEqual(other: Date): Boolean =
    int >= other.int

  def addTime(t: Time): DateTime =
    DateTime.unsafeFromLong(int.toLong << 32 | t.seconds)

  override def toString: String =
    s"Date($year,$month,$day)"

  def order(d: Date): Ordering =
    underlying ?|? d.underlying
}


object Date {
  def apply(year: Short, month: Byte, day: Byte): Date =
    macro Macros.literal

  def unsafe(year: Short, month: Byte, day: Byte): Date =
    new Date(unsafeYmdToInt(year, month, day))

  /* 
   * The Gregorian calendar, which ivory time implements was first
   * introduced in 1582 and adopted across the western world over
   * the next few hundred years (although Russia and Greek were late
   * to the party in the 1900s). It corrects from the Julian Calendar
   * which preceeded it by putting additional constraints on leap
   * days; which were every 4 years in the Julian, but excepted years
   * divisible by 100 but not divisible by 400 in Gregorian. This
   * makes the calendar year 365.2425 days long.
   *
   * Ivory uses a mathematical trick to make date manipulations
   * efficient, internally shifting the start of the year to the
   * first of march, such that leap years are added at the end of
   * the preceeding year. The "epoch date" is thus set at 1600/03/01.
   */

  def isValid(year: Short, month: Byte, day: Byte): Boolean = {
    def divisibleBy(n: Int, divisor: Int) =
      ((n / divisor) * divisor) == n
    def leapYear =
      divisibleBy(year, 4) && (!divisibleBy(year, 100) || divisibleBy(year, 400))

    (year >= 1601 || (year == 1600 && month >= 3)) &&
      year <= 3000 &&
      month >= 1 &&
      month <= 12 &&
      day >= 1 && (
      ((month == 1 ||
        month == 3 ||
        month == 5 ||
        month == 7 ||
        month == 8 ||
        month == 10 ||
        month == 12) && day <= 31) ||
        ((month == 4 ||
          month == 6 ||
          month == 9 ||
          month == 11) && day <= 30) ||
        (month == 2 && day <= 28) ||
        (month == 2 && day == 29 && leapYear)
      )
  }

  def create(year: Short, month: Byte, day: Byte): Option[Date] =
    isValid(year, month, day).option(unsafe(year, month, day))

  def maxValue: Date =
    unsafe(3000.toShort, 12.toByte, 31.toByte)

  def minValue: Date =
    unsafe(1600.toShort, 3.toByte, 1.toByte)

  def min(date1: Date, date2: Date): Date =
    if (date1 isBefore date2) date1 else date2

  def max(date1: Date, date2: Date): Date =
    if (date1 isAfter date2) date1 else date2

  def unsafeFromInt(i: Int): Date =
    new Date(i)

  def fromInt(i: Int): Option[Date] =
    create(((i >>> 16) & 0xffff).toShort, ((i >>> 8) & 0xff).toByte, (i & 0xff).toByte)

  @inline def unsafeYmdToInt(y: Short, m: Byte, d: Byte): Int  =
    (y.toInt << 16) | (m.toInt << 8) | d.toInt

  def fromLocalDate(d: LocalDate): Date =
    unsafe(d.getYear.toShort, d.getMonthOfYear.toByte, d.getDayOfMonth.toByte)

  def listParser: ListParser[Date] = {
    import ListParser._
    for {
      y        <- short
      m        <- short
      d        <- short
      result   <- create(y, m.toByte, d.toByte) match {
        case None => ListParser((position, _) => (position, s"""not a valid date ($y-$m-$d)""").failure)
        case Some(d) => d.point[ListParser]
      }
    } yield result
  }

  implicit def DateEqual: Equal[Date] =
    Equal.equalA

  implicit def DateOrder: Order[Date] =
    Order.order(_ order _)

  implicit def DateOrdering: scala.Ordering[Date] =
    DateOrder.toScalaOrdering

  implicit def DateEncodeJson: EncodeJson[Date] =
    EncodeJson(_.hyphenated.asJson)

  // NOTE There are two decode paths, to handle legacy date format which was
  //      just the encoded int written out as is. This was deprecated in
  //      favour of a human parsable yyyy-MM-dd format.
  implicit def DateDecodeJson: DecodeJson[Date] =
    DecodeJson.optionDecoder(_.as[String].toOption.flatMap(Dates.date), "Date") |||
      DecodeJson.optionDecoder(_.as[Int].toOption.flatMap(fromInt), "Date")

  /**
   * This is not epoch! It will take a long which was created from Date.addSeconds and
   * pull the original Date and seconds out.
   */
  def fromSeconds(s: Long): Option[(Date, Int)] =
    fromInt(((s >>> 32) & 0xffffffff).toInt).map(_ -> (s & 0xffffffff).toInt)

  object Macros extends com.ambiata.ivory.reflect.MacrosCompat {

    def literal(c: Context)(year: c.Expr[Short], month: c.Expr[Byte], day: c.Expr[Byte]): c.Expr[Date] = {
      import c.universe._
      (year, month, day) match {
        case (Expr(Literal(Constant(y: Short))), Expr(Literal(Constant(m: Byte))), Expr(Literal(Constant(d: Byte)))) =>
          create(y, m, d) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid date literal Date($y, $m, $d).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.Date.unsafe($y, $m, $d)")
          }
        /**
         * scaladoc magically manages to pass java.lang.Integer when compiling code so we need to deal with that case
         * Note that the code is still safe and it is not possible to create a Date with 10000000 for the month for example
         * and this case will only be used when running scaladoc
         */
        case (Expr(Literal(Constant(y: Integer))), Expr(Literal(Constant(m: Integer))), Expr(Literal(Constant(d: Integer)))) =>
          create(y.toShort, m.toByte, d.toByte) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid date literal Date(${y.toString}, ${m.toString}, ${d.toString}).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.Date.unsafe(${y.toShort}, ${m.toByte}, ${d.toByte})")
          }
        case _ =>
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(year)}, ${showRaw(month)}, ${showRaw(day)}")
      }
    }
  }
}
