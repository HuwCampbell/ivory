package com.ambiata.ivory.core

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}

/** The feature dictionary is simply a look up of metadata for a given identifier/name. */
case class Dictionary(definitions: List[Definition]) {
  /** The number of definitions in this dictionary. */
  def size: Int =
    definitions.size

  /** Index this dictionaries definitions by FeatureId. */
  val byFeatureId: Map[FeatureId, Definition] =
    definitions.map(d => d.featureId -> d).toMap

  /** Index this dictionaries definitions by an integer feature index.
      Note the intention of this is to be consistent only within a
      single command and that there is no persistent integer index of
      a definition. This is important for portability across repositories
      and even factsets in a repository over time. */
  val byFeatureIndex: Map[Int, Definition] =
    definitions.zipWithIndex.map({ case (d, i) => i -> d }).toMap

  /** Reverse index this dictionary by an integer feature index,
      this is the inverse of byFeatureIndex and the same warnings
      apply. */
  val byFeatureIndexReverse: Map[Definition, Int] =
    definitions.zipWithIndex.toMap

  /** Create a `Dictionary` from `this` only containing features in the specified namespace. */
  def forNamespace(namespace: Name): Dictionary =
    Dictionary(definitions.filter(d => d.featureId.namespace === namespace))

  /** Create a `Dictionary` from `this` only containing the specified features. */
  def forFeatureIds(featureIds: Set[FeatureId]): Dictionary =
    Dictionary(definitions.filter(d => featureIds.contains(d.featureId)))

  /** append the mappings coming from another dictionary */
  def append(other: Dictionary) =
    Dictionary(definitions ++ other.definitions)
}

object Dictionary {
  def empty: Dictionary =
    Dictionary(Nil)
}
