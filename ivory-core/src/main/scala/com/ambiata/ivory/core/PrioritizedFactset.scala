package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class PrioritizedFactset(factsetId: FactsetId, priority: Priority) {
  def globPath: String =
    factsetId.render + "/*/*/*/*/*"

  def render = factsetId.render
}

object PrioritizedFactset {
  def fromFactsets(ids: List[FactsetId]): List[PrioritizedFactset] =
    ids.zipWithIndex.map({ case (id, i) => PrioritizedFactset(id, Priority.unsafe(i.toShort)) })

  def concat(init: List[PrioritizedFactset], tail: List[PrioritizedFactset]): List[PrioritizedFactset] =
    (init ++ tail).zipWithIndex.map({ case (PrioritizedFactset(n, _), p) => PrioritizedFactset(n, Priority.unsafe(p.toShort)) })

  def diff(factsets: List[PrioritizedFactset], other: List[PrioritizedFactset]): List[PrioritizedFactset] =
    factsets.map(_.factsetId).diff(other.map(_.factsetId)).zipWithIndex.map({ case (fid, p) => PrioritizedFactset(fid, Priority.unsafe(p.toShort)) })
}
