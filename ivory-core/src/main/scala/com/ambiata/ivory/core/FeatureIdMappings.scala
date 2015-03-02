package com.ambiata.ivory.core

import scalaz._, Scalaz._

class FeatureIdIndex(val int: Int) extends AnyVal {

  override def toString: String =
    s"FeatureIdIndex($int)"
}

object FeatureIdIndex {

  def apply(int: Int): FeatureIdIndex =
    new FeatureIdIndex(int)
}

case class FeatureIdMappings(private val lookup: Array[FeatureId]) {

  def get(index: FeatureIdIndex): Option[FeatureId] =
    (index.int < lookup.length).option(lookup(index.int))

  def getUnsafe(index: FeatureIdIndex): FeatureId =
    lookup(index.int)

  def featureIds: List[FeatureId] =
    lookup.toList

  def byFeatureIdIndex: Map[FeatureIdIndex, FeatureId] =
    lookup.zipWithIndex.map({ case (fid, idx) => (FeatureIdIndex(idx), fid) }).toMap

  def byFeatureId: Map[FeatureId, FeatureIdIndex] =
    byFeatureIdIndex.toList.map(_.swap).toMap
}

object FeatureIdMappings {

  def apply(features: List[FeatureId]): FeatureIdMappings =
    new FeatureIdMappings(features.toArray)

  def fromDictionary(dictionary: Dictionary): FeatureIdMappings =
    FeatureIdMappings(dictionary.sortedByFeatureId.map(_.featureId))
}
