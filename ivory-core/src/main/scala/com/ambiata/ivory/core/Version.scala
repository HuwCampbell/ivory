package com.ambiata.ivory.core

import com.ambiata.ivory.reflect.MacrosCompat
import com.ambiata.notion.core.KeyName
import com.ambiata.mundane.parse.ListParser
import scalaz._, Scalaz._
import argonaut._, Argonaut._

case class VersionX private (toInt: Int) {
  def render: String =
    s"v${toInt}"

  def next: Option[VersionX] =
    (toInt != Int.MaxValue).option(VersionX(toInt + 1))

  def order(i: VersionX): Ordering =
    (toInt ?|? i.toInt)
}

object VersionX extends MacrosCompat {
  def initial: VersionX =
    VersionX(1)

  def parse(s: String): Option[VersionX] =
    s.startsWith("v").option(s).flatMap(_.tail.parseInt.toOption).filter(_ > 0).map(new VersionX(_))

  implicit def VersionXOrder: Order[VersionX] =
    Order.order(_ order _)

  implicit def VersionXOrdering: scala.Ordering[VersionX] =
    VersionXOrder.toScalaOrdering

  implicit def VersionXEncodeJson: EncodeJson[VersionX] =
    EncodeJson(_.render.asJson)

  implicit def VersionXDecodeJson: DecodeJson[VersionX] =
    DecodeJson.optionDecoder(_.string.flatMap(parse), "VersionX")

  def apply(string: String): VersionX =
    macro versionMacro

  def versionMacro(c: Context)(string: c.Expr[String]): c.Expr[VersionX] = {
    import c.universe._
    string match {
      case Expr(Literal(Constant(str: String))) =>
        VersionX.parse(str).getOrElse(c.abort(c.enclosingPosition, s"Invalid VersionX: $str"))
        c.Expr[VersionX](q"VersionX.parse($str).get")

      case other =>
        c.abort(c.enclosingPosition, s"This is not a valid VersionX string: ${showRaw(string)}")
    }
  }
}
