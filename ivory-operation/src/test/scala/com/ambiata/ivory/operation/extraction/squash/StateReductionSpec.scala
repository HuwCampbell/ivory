package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.arbitraries.UniqueDates
import com.ambiata.ivory.operation.extraction.reduction._
import org.specs2.{ScalaCheck, Specification}

class StateReductionSpec extends Specification with ScalaCheck { def is = s2"""

  Find latest value for state                                      $latest
  Count facts for state                                            $count
"""

  def latest = prop((facts: List[Fact], dates: UniqueDates) => {
    ReducerUtil.updateAll(new StateReduction(dates.earlier, dates.now, new LatestReducer), facts) ====
      facts.sortBy(_.datetime.long).lastOption.filter(!_.isTombstone).map(_.toThrift.getValue).orNull
  })

  def count = prop((allFacts: List[Fact], dates: UniqueDates) => {
    val facts = allFacts.filter(_.date <= dates.now)
    val (before, after) = facts.sortBy(_.datetime.long).partition(!Window.isFactWithinWindowRange(dates.earlier, dates.now, _))
    ReducerUtil.updateAll(new StateReduction(dates.earlier, dates.now, new CountReducer), facts).getL ====
      (before.lastOption.toList ++ after).count(!_.isTombstone)
  })
}
