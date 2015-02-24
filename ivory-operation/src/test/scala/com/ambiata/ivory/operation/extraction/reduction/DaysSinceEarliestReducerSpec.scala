package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class DaysSinceEarliestReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Days since earliest                              $daysSinceEarliest
  Days since earliest laws                         $daysSinceEarliestLaws
"""

  def daysSinceEarliest = prop((fs: FactsWithDate) => {
    val r = new DaysSinceEarliestReducer(fs.offsets)
    r.clear()
    fs.ds.foreach(r.update)
    fs.ds.find(!_.isTombstone).map(f => fs.offsets.untilEnd(f.date).value ==== r.save.getI)
      .getOrElse(r.save must beNull)
  })

  def daysSinceEarliestLaws =
    ReducerUtil.reductionWithDatesLaws(new DaysSinceEarliestReducer(_))
}
