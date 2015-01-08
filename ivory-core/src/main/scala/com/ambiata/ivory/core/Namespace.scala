package com.ambiata.ivory.core

import argonaut._, Argonaut._

import com.ambiata.ivory.reflect.MacrosCompat
import com.ambiata.mundane.parse.ListParser
import com.ambiata.notion.core.KeyName
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._

/**
 * An ivory fact namespace.
 *
 * This is a constrained string:
 *  - It contains only characters: [A-Z][a-z], numbers [0-9] and underscores '_' or dashes '-'
 *  - It cannot start with '_' (to avoid it being interpreted as a hidden file name)
 *  - It must not be empty.
 */
class Namespace private (val name: String) extends AnyVal {
  def asKeyName =
    KeyName.unsafe(name)

  override def toString: String =
    s"Namespace($name)"
}


object Namespace extends MacrosCompat {
  /**
   * use this method to create a Namespace from a String when
   * you are absolutely sure that the string is well-formed
   */
  def reviewed(s: String): Namespace =
    nameFromString(s).getOrElse(Crash.error(Crash.Invariant, s"The name $s was assumed to be well-formed but it isn't"))

  /**
   * use this method to create a Namespace from a String when
   * it could potentially be incorrect
   */
  def unsafe(s: String): Namespace = new Namespace(s.intern())

  def listParser: ListParser[Namespace] =
    ListParser.string.flatMap[Namespace] { s =>
      nameFromString(s).cata(
        n => ListParser.value(n.success),
        ListParser.value(s"$s is not a proper Namespace".failure))
    }

  implicit def NamespaceEqual: Equal[Namespace] =
    Equal.equalA

  implicit def NamespaceOrder: Order[Namespace] =
    Order.orderBy(_.name)

  implicit def NamespaceOrdering: scala.Ordering[Namespace] =
    NamespaceOrder.toScalaOrdering

  implicit def NamespaceCodecJson: CodecJson[Namespace] = CodecJson.derived(
    EncodeJson(_.name.asJson),
    DecodeJson.optionDecoder(_.string.flatMap(nameFromString), "Namespace"))

  def apply(s: String): Namespace =
    macro createNamespaceFromStringMacro

  def fromPathNamespace(path: Path): Namespace =
    new Namespace(path.getName)

  def nameFromString(s: String): Option[Namespace] =
    nameFromStringDisjunction(s).toOption

  def nameFromStringDisjunction(s: String): String \/ Namespace =
    if (s.matches("[A-Za-z0-9_-]+") && !s.startsWith("_") && s.length < 256)
      new Namespace(s.intern()).right
    else
      s"$s is not a valid Namespace".left

  def createNamespaceFromStringMacro(c: Context)(s: c.Expr[String]): c.Expr[Namespace] = {
    import c.universe._
    s match {
      case Expr(Literal(Constant(v: String))) => createNamespaceFromString(c)(v)
      case _ => c.abort(c.enclosingPosition, s"Not a valid literal string ${showRaw(s)}")
    }
  }

  private def createNamespaceFromString(c: Context)(s: String): c.Expr[Namespace] = {
    import c.universe._
    nameFromString(s) match {
      case None     =>
        c.abort(c.enclosingPosition,
          s"""|$s is not a valid name:
              |It must only contains letters: [A-Z][a-z], numbers: [0-9] and _ or -
              |It must not start with _
              |It must not be empty)""".stripMargin)

      case Some(fn) =>
        c.Expr(q"Namespace.unsafe(${fn.name})")
    }
  }
}
