package com.ambiata.ivory.data

import com.ambiata.ivory.reflect.MacrosCompat
import com.ambiata.notion.core._
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._
import argonaut._, Argonaut._

/**
 * TODO Delete this as part of the migration to the new Identifier format
 *      see https://github.com/ambiata/ivory/issues/56
 */
class OldIdentifier private (val n: Int) extends AnyVal {
  def render: String =
    String.format("%05d", java.lang.Integer.valueOf(n))

  def asKeyName =
    KeyName.unsafe(render)

  override def toString: String =
    render

  def toInt: Int =
    n

  def next: Option[OldIdentifier] =
    (n != OldIdentifier.max.n).option(new OldIdentifier(n + 1))

  def order(i: OldIdentifier): Ordering =
    n ?|? i.n
}

object OldIdentifier extends MacrosCompat {

  def initial: OldIdentifier =
    new OldIdentifier(0)

  val max = new OldIdentifier(99999)

  def unsafe(id: Int): OldIdentifier =
    new OldIdentifier(id)

  def fromInt(n: Int): Option[OldIdentifier] =
    if (n > max.n) none else some(new OldIdentifier(n))

  def parse(s: String): Option[OldIdentifier] = try {
    val i = java.lang.Integer.parseInt(s)
    fromInt(i)
  } catch {
    case e: NumberFormatException => None
  }

  def listParser: ListParser[OldIdentifier] = {
    import ListParser._
    for {
      s         <- string
      position  <- getPosition
      result    <- value(parse(s).map(_.success).getOrElse(s"""not an OldIdentifier: '$s'""".failure))
    } yield result
  }

  implicit def OldIdentifierOrder: Order[OldIdentifier] =
    Order.order(_ order _)

  implicit def OldIdentifierOrdering =
    OldIdentifierOrder.toScalaOrdering

  implicit def OldIdentifierCodecJson: CodecJson[OldIdentifier] = CodecJson.derived(
    EncodeJson(_.n.asJson),
    DecodeJson.optionDecoder(_.as[Int].toOption.flatMap(fromInt), "OldIdentifier"))

  def apply(string: String): OldIdentifier =
    macro oldIndentifierMacro

  def oldIndentifierMacro(c: Context)(string: c.Expr[String]): c.Expr[OldIdentifier] = {
    import c.universe._
    string match {
      case Expr(Literal(Constant(str: String))) =>
        Identifier.parse(str).getOrElse(c.abort(c.enclosingPosition, s"Invalid OldIdentifier: $str"))
        c.Expr[OldIdentifier](q"OldIdentifier.parse($str).get")

      case other =>
        c.abort(c.enclosingPosition, s"This is not a valid OldIdentifier string: ${showRaw(string)}")
    }
  }

}
