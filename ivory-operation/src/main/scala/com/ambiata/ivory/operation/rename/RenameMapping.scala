package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core.FeatureId
import com.ambiata.ivory.lookup._

case class RenameMapping(mapping: List[(FeatureId, FeatureId)]) {

  def oldFeatures: List[FeatureId] =
    mapping.map(_._1)

  def newFeatures: List[FeatureId] =
    mapping.map(_._2)
}

object RenameMapping {

  import scala.collection.JavaConverters._

  def toThrift(mapping: RenameMapping, lookup: FeatureIdLookup): FeatureIdMapping =
    new FeatureIdMapping(mapping.mapping.map {
      case (from, to) => from.toString -> new FeatureMappingValue(lookup.ids.get(to.toString), to.name)
    }.toMap.asJava)
}
