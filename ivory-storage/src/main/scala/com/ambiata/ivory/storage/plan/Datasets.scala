package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._

case class Datasets(sets: List[Prioritized[Dataset]]) {

  def byPriority: Datasets =
    copy(sets = sets.sorted)

  def ++(other: Datasets): Datasets =
    copy(sets = sets ++ other.sets)

  def +:(elem: Prioritized[Dataset]): Datasets =
    copy(sets = elem +: sets)

  def add(priority: Priority, dataset: Dataset): Datasets =
    copy(sets = Prioritized(priority, dataset) +: sets)

  def filter(f: Dataset => Boolean): Datasets =
    copy(sets = sets.filter(e => f(e.value)))
}
