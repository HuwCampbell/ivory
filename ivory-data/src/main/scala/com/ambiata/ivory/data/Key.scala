package com.ambiata.ivory.data

import scalaz._, Scalaz._

class Key private (underlying: String) {
  def render = underlying

  override def toString = render

  override def equals(a: Any) = a match {
    case other: Key => render == other.render
    case _          => false
  }
  override def hashCode = render.hashCode
}

object Key extends com.ambiata.ivory.reflect.MacrosCompat {

  def apply(string: String): Key =
   macro keyMacro

  def keyMacro(c: Context)(string: c.Expr[String]): c.Expr[Key] = {
    import c.universe._
    string match {
      case Expr(Literal(Constant(str: String))) =>
        Key.create(str).getOrElse(c.abort(c.enclosingPosition, s"Invalid Key: $str"))
        c.Expr[Key](q"Key.create($str).get")

      case other =>
        c.abort(c.enclosingPosition, s"This is not a valid key string: ${showRaw(string)}")
    }
  }

  def create(s: String): Option[Key] =
    (!s.isEmpty && s.forall(c => c.isLetterOrDigit || c == '-' || c == '_' || c == '.')).option(new Key(s))
}
