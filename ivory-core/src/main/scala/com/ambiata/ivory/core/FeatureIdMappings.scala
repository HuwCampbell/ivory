package com.ambiata.ivory.core

import scalaz._, Scalaz._

case class FeatureIdIndex(int: Int) extends AnyVal

case class FeatureIdMappings(private val lookup: Array[FeatureId]) {

  def get(index: FeatureIdIndex): Option[FeatureId] =
    (index.int < lookup.length).option(lookup(index.int))

  def getUnsafe(index: FeatureIdIndex): FeatureId =
    lookup(index.int)

  def featureIds: List[FeatureId] =
    lookup.toList

  val byFeatureId: Map[FeatureId, FeatureIdIndex] =
    lookup.zipWithIndex.map({ case (fid, idx) => (fid, FeatureIdIndex(idx)) }).toMap
}

object FeatureIdMappings {

  def apply(features: List[FeatureId]): FeatureIdMappings =
    new FeatureIdMappings(features.toArray)

  def fromDictionary(dictionary: Dictionary): FeatureIdMappings =
    FeatureIdMappings(dictionary.sortedByFeatureId.map(_.featureId))
}
