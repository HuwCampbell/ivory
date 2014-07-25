package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class Prioritized[A](priority: Priority, value: A)

object Prioritized {

  implicit def PrioritizedOrder[A]: Order[Prioritized[A]] =
    Order.orderBy(_.priority)

  implicit def ProritizedOrdering[A]: scala.Ordering[Prioritized[A]] =
    PrioritizedOrder.toScalaOrdering
}
