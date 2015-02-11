package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._

class FeatureIdMappingsSpec extends Specification with ScalaCheck { def is = s2"""

  Can get all FeatureIds                                         $getAll
  Can unsafely get all FeatureIds                                $getUnsafe
  By feature id contains matching entries                        $byFeatureId
  Can create a mapping from a dictionary                         $fromDictionary

"""

  def getAll = prop((ids: List[FeatureId]) => {
    val mappings = FeatureIdMappings(ids)
    seqToResult(ids.zipWithIndex.map({ case (featureId, i) =>
      mappings.get(FeatureIdIndex(i)) must beSome(featureId)
    }))
  })

  def getUnsafe = prop((ids: List[FeatureId]) => {
    val mappings = FeatureIdMappings(ids)
    seqToResult(ids.zipWithIndex.map({ case (featureId, i) =>
      mappings.getUnsafe(FeatureIdIndex(i)) ==== featureId
    }))
  })

  def byFeatureId = prop((ids: List[FeatureId]) => {
    val byFeatureId: Map[FeatureId, FeatureIdIndex] = FeatureIdMappings(ids).byFeatureId
    seqToResult(ids.zipWithIndex.map({ case (featureId, i) =>
      byFeatureId.get(featureId) must beSome(FeatureIdIndex(i))
    }))
  })

  def fromDictionary = prop((dictionary: Dictionary) =>
    FeatureIdMappings.fromDictionary(dictionary).featureIds.sorted ==== dictionary.definitions.map(_.featureId).sorted)
}
