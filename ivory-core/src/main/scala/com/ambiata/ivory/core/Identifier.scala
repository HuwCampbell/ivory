package com.ambiata.ivory.core

import com.ambiata.ivory.reflect.MacrosCompat
import com.ambiata.notion.core.KeyName
import com.ambiata.mundane.parse.ListParser
import scalaz._, Scalaz._
import argonaut._, Argonaut._

class Identifier private (val toInt: Int, val tag: IdentifierTag) {
  def render: String = tag match {
    case IdentifierV1 =>
      String.format("%05d", java.lang.Integer.valueOf(toInt))
    case IdentifierV2 =>
      String.format("%08x", java.lang.Integer.valueOf(toInt))
  }

  def asKeyName =
    KeyName.unsafe(render)

  override def toString: String =
    s"Identifier($render, $tag)"

  def next: Option[Identifier] = tag match {
    case IdentifierV1 =>
      Identifier.initial.some
    case IdentifierV2 =>
      (toInt != 0xffffffff).option(new Identifier(toInt + 1, tag))
  }

  def order(i: Identifier): Ordering =
    (tag ?|? i.tag) |+| (toInt ?|? i.toInt)

  override def hashCode: Int =
    toInt * 31

  override def equals(o: Any): Boolean =
    o.isInstanceOf[Identifier] && {
      val x = o.asInstanceOf[Identifier]
      x.toInt == toInt && x.tag == tag
    }
}

object Identifier extends MacrosCompat {
  def initial: Identifier =
    new Identifier(0, IdentifierV2)

  def unsafe(i: Int): Identifier =
    new Identifier(i, IdentifierV2)

  def unsafeV1(i: Int): Identifier =
    new Identifier(i, IdentifierV1)

  def parse(s: String): Option[Identifier] =
    parseV2(s) orElse parseV1(s)

  def parseV1(s: String): Option[Identifier] = try {
    val i = java.lang.Integer.parseInt(s)
    (i <= 99999 && s.length == 5).option(new Identifier(i, IdentifierV1))
  } catch {
    case e: NumberFormatException => None
  }
  def parseV2(s: String): Option[Identifier] = try {
    val l = java.lang.Long.parseLong(s, 16)
    (l <= 0xffffffffL && s.length == 8).option(new Identifier(l.toInt, IdentifierV2))
  } catch {
    case e: NumberFormatException => None
  }

  def listParser: ListParser[Identifier] = {
    import ListParser._
    for {
      s         <- string
      result    <- value(parse(s).map(_.success).getOrElse(s"""not an Identifier: '$s'""".failure))
    } yield result
  }

  implicit def IdentifierOrder: Order[Identifier] =
    Order.order(_ order _)

  implicit def IdentifierOrdering =
    IdentifierOrder.toScalaOrdering

  implicit def IdentifierCodecJson: CodecJson[Identifier] =
    CodecJson.derived(
      EncodeJson(_.render.asJson),
      DecodeJson.optionDecoder((_.as[String].toOption.flatMap(s => parse(s))), "Identifier"))

  def apply(string: String): Identifier =
    macro identifierMacro

  def identifierMacro(c: Context)(string: c.Expr[String]): c.Expr[Identifier] = {
    import c.universe._
    string match {
      case Expr(Literal(Constant(str: String))) =>
        Identifier.parse(str).getOrElse(c.abort(c.enclosingPosition, s"Invalid Identifier: $str"))
        c.Expr[Identifier](q"Identifier.parse($str).get")

      case other =>
        c.abort(c.enclosingPosition, s"This is not a valid Identifier string: ${showRaw(string)}")
    }
  }

}


sealed trait IdentifierTag
case object IdentifierV1 extends IdentifierTag
case object IdentifierV2 extends IdentifierTag

object IdentifierTag {
  implicit def IdentifierTagOrder: Order[IdentifierTag] =
    Order.order((a, b) => (a, b) match {
      case (IdentifierV1, IdentifierV1) => Ordering.EQ
      case (IdentifierV1, IdentifierV2) => Ordering.LT

      case (IdentifierV2, IdentifierV1) => Ordering.GT
      case (IdentifierV2, IdentifierV2) => Ordering.EQ
    })

  implicit def IdentifierTagOrdering: scala.Ordering[IdentifierTag] =
    IdentifierTagOrder.toScalaOrdering
}
