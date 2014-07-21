package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class PrioritizedFactset(set: Factset, priority: Priority) {
  def globPath: String =
    set.name + "/*/*/*/*/*"
}

object PrioritizedFactset {
  def fromFactsets(sets: List[Factset]): List[PrioritizedFactset] =
    sets.zipWithIndex.map({ case (set, i) => PrioritizedFactset(set, Priority.unsafe(i.toShort)) })

  def concat(init: List[PrioritizedFactset], tail: List[PrioritizedFactset]): List[PrioritizedFactset] =
    (init ++ tail).zipWithIndex.map({ case (PrioritizedFactset(n, _), p) => PrioritizedFactset(n, Priority.unsafe(p.toShort)) })

  def diff(factsets: List[PrioritizedFactset], other: List[PrioritizedFactset]): List[PrioritizedFactset] =
    factsets.map(_.set.name).diff(other.map(_.set.name)).zipWithIndex.map({ case (n, p) => PrioritizedFactset(Factset(n), Priority.unsafe(p.toShort)) })
}
