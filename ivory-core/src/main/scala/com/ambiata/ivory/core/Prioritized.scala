package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class Prioritized[A](priority: Priority, value: A) {
  def map[B](f: A => B): Prioritized[B] =
    copy(value = f(value))
}

object Prioritized {
  implicit def PrioritizedOrder[A]: Order[Prioritized[A]] =
    Order.orderBy(_.priority)

  implicit def ProritizedOrdering[A]: scala.Ordering[Prioritized[A]] =
    PrioritizedOrder.toScalaOrdering

  implicit def PrioritizedFunctor: Functor[Prioritized] = new Functor[Prioritized] {
    def map[A, B](p: Prioritized[A])(f: A => B) = p.map(f)
  }

  def fromInt[A](i: Int, a: A): Option[Prioritized[A]] =
    Priority.parseInt(i).map(p => Prioritized(p, a))

  def fromList[A](list: List[A]): Option[List[Prioritized[A]]] =
    list.zipWithIndex.traverse({ case (a, i) => Prioritized.fromInt(i + 1, a) })
}
