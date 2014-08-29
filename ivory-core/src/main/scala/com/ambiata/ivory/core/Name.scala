package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import com.ambiata.ivory.reflect.MacrosCompat
import com.ambiata.mundane.parse.ListParser
import org.apache.hadoop.fs.Path

import scalaz.{Name => _,_}, Scalaz._

/**
 * A Name is a constrained string which can be used
 * to give a "simple" name to things.
 *
 * It contains only characters: [A-Z][a-z], numbers [0-9] and underscores '_' or dashes '-'
 * It cannot start with '_' (to avoid it being interpreted as a hidden file name)
 * It must not be empty.
 *
 * As a result it can be used to create a file name
 */
class Name private(val name: String) extends AnyVal {
  def asFileName = FileName.unsafe(name)
  def asDirPath = DirPath.unsafe(name)
  def asFilePath = FilePath.unsafe(name)
}

object Name extends MacrosCompat {

  /**
   * use this method to create a Name from a String when
   * you are absolutely sure that the string is well-formed
   */
  def reviewed(s: String): Name =
    nameFromString(s).getOrElse(Crash.error(Crash.Invariant, s"The name $s was assumed to be well-formed but it isn't"))

  /**
   * use this method to create a Name from a String when
   * it could potentially be incorrect
   */
  def unsafe(s: String): Name = new Name(s)

  def listParser: ListParser[Name] =
    ListParser.string.flatMap[Name] { s =>
      nameFromString(s).cata(
        n => ListParser.value(n.success),
        ListParser.value(s"$s is not a proper Name".failure))
    }


  implicit def EqualName: Equal[Name] = new Equal[Name] {
    def equal(a1: Name, a2: Name): Boolean =
     a1 == a2
  }

  implicit def NameOrder: Order[Name] =
    Order.order((n1: Name, n2: Name) => implicitly[Order[String]].order(n1.name, n2.name))

  implicit def NameOrdering: scala.Ordering[Name] =
    NameOrder.toScalaOrdering

  def apply(s: String): Name =
    macro createNameFromStringMacro

  def fromPathName(path: Path): Name =
    new Name(path.getName)

  def nameFromString(s: String): Option[Name] =
    nameFromStringDisjunction(s).toOption

  def nameFromStringDisjunction(s: String): String \/ Name =
    if (s.matches("([A-Z]|[a-z]|[0-9]|\\-|_)+") && !s.startsWith("_")) (new Name(s)).right
    else                                                               s"$s is not a valid Name".left

  def createNameFromStringMacro(c: Context)(s: c.Expr[String]): c.Expr[Name] = {
    import c.universe._
    s match {
      case Expr(Literal(Constant(v: String))) => createNameFromString(c)(v)
      case _ => c.abort(c.enclosingPosition, s"Not a valid literal string ${showRaw(s)}")
    }
  }

  private def createNameFromString(c: Context)(s: String): c.Expr[Name] = {
    import c.universe._
    nameFromString(s) match {
      case None     =>
        c.abort(c.enclosingPosition,
          s"""|$s is not a valid name:
              |It must only contains letters: [A-Z][a-z], numbers: [0-9] and _ or -
              |It must not start with _
              |It must not be empty")""".stripMargin)

      case Some(fn) =>
        c.Expr(q"Name.unsafe(${fn.name})")
    }
  }
}
