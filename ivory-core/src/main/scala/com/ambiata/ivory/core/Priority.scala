package com.ambiata.ivory.core

import scalaz._, Scalaz._
import org.joda.time.LocalDate
import com.ambiata.mundane.parse.ListParser

class Priority private(val toShort: Short) extends AnyVal {
  override def toString: String =
    s"Priority($toShort)"

  def next: Option[Priority] =
    Priority.parseInt(toShort + 1)
}

object Priority {
  def Min =
    unsafe(0)

  def Max =
    unsafe(Short.MaxValue)

  implicit def PriorityOrder: Order[Priority] =
    Order.orderBy(_.toShort)

  implicit def ProrityOrdering =
    PriorityOrder.toScalaOrdering

  def apply(priority: Short): Priority =
    macro Macros.literal

  def unsafe(priority: Short): Priority =
    new Priority(priority)

  def create(priority: Short): Option[Priority] =
    (priority >= 0).option(unsafe(priority))

  def parseInt(priority: Int): Option[Priority] =
    (priority <= Max.toShort).option(create(priority.toShort)).flatten

  object Macros extends com.ambiata.ivory.reflect.MacrosCompat {

    def literal(c: Context)(priority: c.Expr[Short]): c.Expr[Priority] = {
      import c.universe._
      priority match {
        case Expr(Literal(Constant(p: Short))) =>
          create(p) match {
            case None =>
              c.abort(c.enclosingPosition, s"This is not a valid priority literal Priority($p).")
            case Some(_) =>
              c.Expr(q"com.ambiata.ivory.core.Priority.unsafe($p)")
          }
        /**
         * scaladoc magically manages to pass java.lang.Integer when compiling code so we need to deal with that case
         */
        case Expr(Literal(Constant(p: Int))) =>
          if (p <= Short.MaxValue) {
            create(p.toShort) match {
              case None =>
                c.abort(c.enclosingPosition, s"This is not a valid priority literal Priority($p).")
              case Some(_) =>
                c.Expr(q"com.ambiata.ivory.core.Priority.unsafe($p)")
            }
          } else c.abort(c.enclosingPosition, s"Not a short ${showRaw(priority)}.")
        case _ =>
          c.abort(c.enclosingPosition, s"Not a literal ${showRaw(priority)}.")
      }
    }
  }
}
