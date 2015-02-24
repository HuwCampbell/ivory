package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

import scala.collection.JavaConverters._

class CountBySecondaryReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the count of a key and a secondary field                  $countBySecondary
  Count by secondary laws                                        $countBySecondaryLaws
"""

  def countBySecondary = prop((l: List[(String, List[String])]) => {
    // ScalaCheck discards too many values if we use Map/Set directly :(
    val xs = l.toMap.mapValues(_.toSet)
    val xss = xs.toList.flatMap(x => x._2.map(x._1 ->))
    ReducerUtil.run2(new CountBySecondaryReducer[String, String], xss).map.asScala.toMap.mapValues(_.toSet) ==== xs.filter(_._2.nonEmpty)
  })

  def countBySecondaryLaws =
    ReducerUtil.reductionFold2Laws(new CountBySecondaryReducer[Boolean, Short])
}
