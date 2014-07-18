package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import scalaz.{Value => _, _}, Scalaz._

object FeatureStoreTextStorage extends TextStorage[PrioritizedFactset, FeatureStore] {

  val name = "feature store"

  def fromList(sets: List[PrioritizedFactset]) =
    FeatureStore(sets)

  def toList(store: FeatureStore): List[PrioritizedFactset] =
    store.factsets.sortBy(_.priority)

  def parseLine(i: Int, l: String): ValidationNel[String, PrioritizedFactset] = {
    val trimmed = l.trim
    if (trimmed.matches("\\s")) s"Line number $i '$l' contains white space.".failureNel
    else PrioritizedFactset(Factset(l), Priority.unsafe(i.toShort)).success
  }

  def toLine(f: PrioritizedFactset): String =
    f.set.name

  def fromFactsets(sets: List[Factset]): FeatureStore =
    FeatureStore(PrioritizedFactset.fromFactsets(sets))
}
