package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class DaysSinceLatestByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the days since latest by a number of facts      $daysSinceLatestBy
  Days since latest by reduction laws                       $daysSinceLatestByLaws
"""

  def daysSinceLatestBy = prop((xs: ValuesWithDate[String]) => {
    ReducerUtil.runWithDates(new DaysSinceLatestByReducer(xs.offsets), xs.ds).map.asScala.toMap ====
      xs.ds.groupBy(_._1).mapValues(ds => xs.offsets.untilEnd(ds.map(_._2).sorted.last).value)
  })

  def daysSinceLatestByLaws =
    ReducerUtil.reductionFoldWithDateLaws(offsets => new DaysSinceLatestByReducer(offsets))
}
