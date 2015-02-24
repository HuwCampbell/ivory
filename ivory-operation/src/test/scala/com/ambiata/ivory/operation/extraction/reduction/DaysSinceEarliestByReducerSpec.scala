package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

import scala.collection.JavaConverters._

class DaysSinceEarliestByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the days since earliest of a number of facts    $daysSinceEarliestBy
  Days since earliest by reducer laws                       $daysSinceEarliestByLaws
"""

  def daysSinceEarliestBy = prop((xs: ValuesWithDate[String]) => {
    ReducerUtil.runWithDates(new DaysSinceEarliestByReducer(xs.offsets), xs.ds).map.asScala.toMap ====
      xs.ds.groupBy(_._1).mapValues(ds => xs.offsets.untilEnd(ds.map(_._2).sorted.head).value)
  })

  def daysSinceEarliestByLaws =
    ReducerUtil.reductionFoldWithDateLaws(offsets => new DaysSinceEarliestByReducer(offsets))
}
