package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

import scala.collection.JavaConverters._

class CountByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the count of an arbitrary values                       $countBy
  Ensure that the sum of counts matches the number of facts   $countBySum
  Count by laws                                               $countByLaws
"""

  def countBy = prop((l: List[String]) => {
    ReducerUtil.run(new CountByReducer[String], l).map.asScala.toMap ==== l.groupBy(identity).mapValues(_.size)
  })

  def countBySum = prop((l: List[String]) => {
    ReducerUtil.run(new CountByReducer[String], l).map.asScala.values.sum ==== l.size
  })

  def countByLaws =
    ReducerUtil.reductionFoldLaws(new CountByReducer[String])
}
