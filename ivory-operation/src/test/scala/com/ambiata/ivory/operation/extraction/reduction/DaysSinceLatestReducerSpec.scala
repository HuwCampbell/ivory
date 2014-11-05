package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}

class DaysSinceLatestReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Days since latest                                $daysSinceLatest
"""

  def daysSinceLatest = prop((fs: List[Fact]) => {
    val facts = fs.filter(!_.isTombstone).sortBy(_.date)
    val dateOffsets = DateOffsets.compact(
      facts.headOption.map(_.date).getOrElse(Date.minValue),
      facts.lastOption.map(_.date).getOrElse(Date.minValue)
    )
    val r = new DaysSinceLatestReducer(dateOffsets)
    r.clear()
    facts.foreach(r.update)
    facts.lastOption.map(f => dateOffsets.untilEnd(f.date).value)
      .map(_ ==== r.save.getI).getOrElse(r.save must beNull)
  }).set(minTestsOk = 10)
}
