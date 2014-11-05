package com.ambiata.ivory.core

case class Datasets(sets: List[Prioritized[Dataset]]) {
  def byPriority: Datasets =
    copy(sets = sets.sorted)

  def ++(other: Datasets): Datasets =
    copy(sets = sets ++ other.sets)

  def +:(elem: Prioritized[Dataset]): Datasets =
    copy(sets = elem +: sets)

  def add(priority: Priority, dataset: Dataset): Datasets =
    Prioritized(priority, dataset) +: this

  def filter(f: Dataset => Boolean): Datasets =
    copy(sets = sets.filter(e => f(e.value)))

  def prune: Datasets =
    Datasets(sets.filter(p => !p.value.isEmpty))
}

object Datasets {
  def empty: Datasets =
    Datasets(Nil)
}
