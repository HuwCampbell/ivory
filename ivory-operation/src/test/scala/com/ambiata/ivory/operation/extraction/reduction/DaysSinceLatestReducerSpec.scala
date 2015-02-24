package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class DaysSinceLatestReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Days since latest                                $daysSinceLatest
  Days since latest reducer laws                   $daysSinceLatestLaws
"""

  def daysSinceLatest = prop((fs: FactsWithDate) => {
    val r = new DaysSinceLatestReducer(fs.offsets)
    r.clear()
    fs.ds.foreach(r.update)
    fs.ds.filter(!_.isTombstone).lastOption.map(f => fs.offsets.untilEnd(f.date).value)
      .map(_ ==== r.save.getI).getOrElse(r.save must beNull)
  }).set(minTestsOk = 10)

  def daysSinceLatestLaws =
    ReducerUtil.reductionWithDatesLaws(new DaysSinceLatestReducer(_))
}
