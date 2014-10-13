package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

import scala.collection.JavaConverters._

class CountByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the count of an arbitrary values                       $countBy
"""

  def countBy = prop((xs: List[String]) =>
    ReducerUtil.run(new CountByReducer[String], xs).map.asScala.toMap ==== xs.groupBy(identity).mapValues(_.length.toLong)
  )
}
