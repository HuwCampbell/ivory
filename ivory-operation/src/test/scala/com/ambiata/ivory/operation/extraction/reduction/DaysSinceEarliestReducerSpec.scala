package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class DaysSinceEarliestReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Days since earliest                              $daysSinceEarliest
"""

  def daysSinceEarliest = prop((fs: List[Fact]) => {
    val facts = fs.filter(!_.isTombstone).sortBy(_.date)
    val dateOffsets = DateOffsets.compact(
      facts.headOption.map(_.date).getOrElse(Date.minValue),
      facts.lastOption.map(_.date).getOrElse(Date.minValue)
    )
    val r = new DaysSinceEarliestReducer(dateOffsets)
    r.clear()
    facts.foreach(r.update)
    facts.headOption.map(f => dateOffsets.untilEnd(f.date).value ==== r.save.getI)
      .getOrElse(r.save must beNull)
  })
}
