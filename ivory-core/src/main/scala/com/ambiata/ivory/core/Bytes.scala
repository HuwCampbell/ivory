package com.ambiata.ivory.core

import scalaz._, Scalaz._
import argonaut._, Argonaut._

class Bytes(val toLong: Long) extends AnyVal {
  def +(x: Bytes): Bytes =
    Bytes(toLong + x.toLong)

  def -(x: Bytes): Bytes =
    Bytes(toLong - x.toLong)

  override def toString: String =
    s"Bytes($toLong)"
}

object Bytes {
  def apply(v: Long): Bytes =
    new Bytes(v)

  implicit def BytesEqual: Equal[Bytes] =
    Equal.equalBy(_.toLong)

  implicit def BytesOrder: Order[Bytes] =
    Order.orderBy(_.toLong)

  implicit def BytesMonoid: Monoid[Bytes] =
    Monoid.instance((a, b) => Bytes(a.toLong + b.toLong), Bytes(0))

  implicit def BytesOrdering: scala.Ordering[Bytes] =
    BytesOrder.toScalaOrdering

  implicit def BytesCodecJson: CodecJson[Bytes] =
    CodecJson.derived[Long].xmap(new Bytes(_))(_.toLong)
}
