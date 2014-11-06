package com.ambiata.ivory.core

import scalaz._, Scalaz._

/** The feature dictionary is simply a look up of metadata for a given identifier/name. */
case class Dictionary(definitions: List[Definition]) {
  /** The number of definitions in this dictionary. */
  def size: Int =
    definitions.size

  val sortedByFeatureId: List[Definition] =
    definitions.sortBy(_.featureId)

  /** Index this dictionaries definitions by FeatureId. */
  val byFeatureId: Map[FeatureId, Definition] =
    definitions.map(d => d.featureId -> d).toMap

  /** Index this dictionaries definitions by an integer feature index.
      Note the intention of this is to be consistent only within a
      single command and that there is no persistent integer index of
      a definition. This is important for portability across repositories
      and even factsets in a repository over time. */
  val byFeatureIndex: Map[Int, Definition] =
    sortedByFeatureId.zipWithIndex.map(_.swap).toMap

  /** Reverse index this dictionary by an integer feature index,
      this is the inverse of byFeatureIndex and the same warnings
      apply. */
  val byFeatureIndexReverse: Map[FeatureId, Int] =
    sortedByFeatureId.map(_.featureId).zipWithIndex.toMap

  /** Create a `Dictionary` from `this` only containing features in the specified namespace. */
  def forNamespace(namespace: Name): Dictionary =
    Dictionary(definitions.filter(d => d.featureId.namespace === namespace))

  /** Create a `Dictionary` from `this` only containing the specified features. */
  def forFeatureIds(featureIds: Set[FeatureId]): Dictionary =
    Dictionary(definitions.filter(d => featureIds.contains(d.featureId)))

  def byConcrete: DictionaryConcrete = DictionaryConcrete(
    definitions.map {
      case Concrete(fid, cd) => fid       -> None
      case Virtual(fid, vd)  => vd.source -> (fid, vd).some
    }.groupBy(_._1).flatMap {
      case (fid, cg) =>
        byFeatureId.get(fid).flatMap {
          case Concrete(_, cd) => (fid, ConcreteGroup(cd, cg.map(_._2).flatten)).some
          // Currently we don't support virtual sourcing another virtual
          case Virtual(_, _)   => none
        }
    }
  )

  /** Return `true` if any of the definitions are virtual */
  def hasVirtual: Boolean =
    definitions.exists(_.fold((_, _) => false, (_, _) => true))

  /** append the mappings coming from another dictionary */
  def append(other: Dictionary) =
    Dictionary(definitions ++ other.definitions)

  /** Only required until chord supports windows as well */
  def removeVirtualFeatures: Dictionary =
    Dictionary(definitions.filter({
      case Concrete(_, _) => true
      case Virtual(_, _)  => false
    }))
}

object Dictionary {
  val empty: Dictionary =
    Dictionary(Nil)

  def reduce(dictionaries: List[Dictionary]) =
    dictionaries.foldLeft(Dictionary.empty)(_ append _)
}

/** Represents a dictionary grouped by the concrete definitions */
case class DictionaryConcrete(sources: Map[FeatureId, ConcreteGroup])
case class ConcreteGroup(definition: ConcreteDefinition, virtual: List[(FeatureId, VirtualDefinition)])
