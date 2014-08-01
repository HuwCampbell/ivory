package com.ambiata.ivory.data

import com.ambiata.mundane.parse.ListParser
import scalaz._, Scalaz._

/*
 * TODO Delete this as part of the migration to the new Identifier format
 *      see https://github.com/ambiata/ivory/issues/56
 */
class OldIdentifier private (val n: Int) extends AnyVal {
  def render: String =
    String.format("%05d", java.lang.Integer.valueOf(n))

  override def toString: String =
    render

  def toInt: Int =
    n

  def next: Option[OldIdentifier] =
    (n != OldIdentifier.max.n).option(new OldIdentifier(n + 1))

  def order(i: OldIdentifier): Ordering =
    n ?|? i.n
}

object OldIdentifier {
  def initial: OldIdentifier =
    new OldIdentifier(0)

  val max = new OldIdentifier(99999)

  def parse(s: String): Option[OldIdentifier] = try {
    val i = java.lang.Integer.parseInt(s)
    if (i > max.n) None else Some(new OldIdentifier(i))
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
}
