package com.ambiata.ivory.core

import com.ambiata.ivory.reflect.MacrosCompat
import com.ambiata.notion.core.KeyName
import com.ambiata.mundane.parse.ListParser
import scalaz._, Scalaz._
import argonaut._, Argonaut._

class Identifier private (val n: Int) extends AnyVal {
  def render: String =
    String.format("%08x", java.lang.Integer.valueOf(n))

  def asKeyName =
    KeyName.unsafe(render)

  override def toString: String =
    render

  def toInt: Int =
    n

  def next: Option[Identifier] =
    (n != 0xffffffff).option(new Identifier(n + 1))

  def order(i: Identifier): Ordering =
    n ?|? i.n
}

object Identifier extends MacrosCompat {
  def initial: Identifier =
    new Identifier(0)

  def unsafe(i: Int): Identifier =
    new Identifier(i)

  def parse(s: String): Option[Identifier] = try {
    val l = java.lang.Long.parseLong(s, 16)
    if (l > 0xffffffffL) None else Some(new Identifier(l.toInt))
  } catch {
    case e: NumberFormatException => None
  }

  implicit def IdentifierOrder: Order[Identifier] =
    Order.order(_ order _)

  implicit def IdentifierOrdering =
    IdentifierOrder.toScalaOrdering

  implicit def IdentifierCodecJson: CodecJson[Identifier] =
    CodecJson.derived(
      EncodeJson(_.toString.asJson),
      DecodeJson.optionDecoder((_.as[String].toOption.flatMap(parse(_))), "Identifier"))

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


  def listParser: ListParser[Identifier] = {
    import ListParser._
    for {
      s         <- string
      result    <- value(parse(s).map(_.success).getOrElse(s"""not an Identifier: '$s'""".failure))
    } yield result
  }
}
