package com.ambiata.ivory.core

import argonaut._, Argonaut._
import org.joda.time.{DateTime => JodaDateTime, DateTimeZone}

import scalaz._, Scalaz._, effect.IO

/** a packed long | 16 bits: year represented as a short | 8 bits: month represented as a byte | 8 bits: day represented as a byte | 32 bits: seconds since start of day */
class DateTime private(val underlying: Long) extends AnyVal {
  def date: Date =
    Date.unsafeFromInt((underlying >>> 32).toInt)

  def time: Time =
    Time.unsafe((underlying & 0xffffffff).toInt)

  def zip: (Date, Time) =
    date -> time

  def long: Long =
    underlying

  def joda(z: DateTimeZone): JodaDateTime =
    new JodaDateTime(date.year.toInt, date.month.toInt, date.day.toInt, time.hours, time.minuteOfHour, time.secondOfMinute, z)

  def iso8601(z: DateTimeZone): String =
    joda(z).toString("yyyy-MM-dd'T'HH:mm:ssZ")

  def localIso8601: String =
    s"${date.hyphenated}T${time.hhmmss}"

  override def toString: String =
    s"DateTime(${date.year},${date.month},${date.day},$time)"

  def isDstOverlap(z: DateTimeZone): Boolean = {
    val e = joda(z).withEarlierOffsetAtOverlap
    val l = e.withLaterOffsetAtOverlap
    e.getMillis != l.getMillis
  }
}

object DateTime {

  def apply(year: Short, month: Byte, day: Byte, seconds: Int): DateTime =
    macro Macros.literal

  def unsafe(year: Short, month: Byte, day: Byte, seconds: Int): DateTime =
    new DateTime(Date.unsafe(year, month, day).int.toLong << 32 | seconds)

  def create(year: Short, month: Byte, day: Byte, seconds: Int): Option[DateTime] = for {
    d <- Date.create(year, month, day)
    t <- Time.create(seconds)
  } yield new DateTime(d.int.toLong << 32 | t.seconds)

  def unsafeFromLong(l: Long): DateTime =
    new DateTime(l)

  def fromJoda(dt: JodaDateTime): DateTime =
    unsafe(dt.getYear.toShort, dt.getMonthOfYear.toByte, dt.getDayOfMonth.toByte, dt.getSecondOfDay)

  def now: IO[DateTime] =
    IO(fromJoda(JodaDateTime.now()))

  implicit def DateTimeOrder: Order[DateTime] =
    Order.order(_.underlying ?|? _.underlying)

  implicit def DateTimeCodecJson: CodecJson[DateTime] = CodecJson.derived(
    EncodeJson(_.iso8601(DateTimeZone.UTC).asJson),
    DecodeJson.optionDecoder(_.string.flatMap(Dates.datetimezone(_, DateTimeZone.UTC)), "DateTime"))

  object Macros extends com.ambiata.ivory.reflect.MacrosCompat {

    def literal(c: Context)(year: c.Expr[Short], month: c.Expr[Byte], day: c.Expr[Byte], seconds: c.Expr[Int]): c.Expr[DateTime] = {
      import c.universe._
      (year, month, day, seconds) match {
        case (Expr(Literal(Constant(y: Short))), Expr(Literal(Constant(m: Byte))), Expr(Literal(Constant(d: Byte))), Expr(Literal(Constant(s: Int)))) =>
          create(y, m, d, s) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid datetime literal DateTime($y, $m, $d, $s).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.DateTime.unsafe($y, $m, $d, $s)")
          }
        /**
         * scaladoc magically manages to pass java.lang.Integer when compiling code so we need to deal with that case
         * Note that the code is still safe and it is not possible to create a Date with 10000000 for the month for example
         * and this case will only be used when running scaladoc
         */
        case (Expr(Literal(Constant(y: Integer))), Expr(Literal(Constant(m: Integer))), Expr(Literal(Constant(d: Integer))), Expr(Literal(Constant(s: Integer)))) =>
          create(y.toShort, m.toByte, d.toByte, s) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid datetime literal DateTime($y, $m, $d, $s).")
            case Some(date) =>
              c.Expr(q"com.ambiata.ivory.core.DateTime.unsafe(${y.toShort}, ${m.toByte}, ${d.toByte}, ${s.toInt})")
          }
        case _ => {
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(year)}, ${showRaw(month)}, ${showRaw(day)}, ${showRaw(seconds)}")
        }
      }
    }
  }
}
