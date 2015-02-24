package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}

class MinimumInDaysReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the minimum in days                                      $minimumInDays
  Default should be zero                                             $minimumInDaysDefault
  Minimum in days reducer laws                                       $minimumInDaysLaws
"""

  def minimumInDays = prop((doc: DatesOfCount) => {
    ReducerUtil.reduceDates(doc, new MinimumInDaysReducer) ==== doc.dates.map(_._1).min
  })

  def minimumInDaysDefault = {
    ReducerUtil.reduceDates(DatesOfCount(Nil), new MinimumInDaysReducer) ==== 0
  }

  def minimumInDaysLaws =
    ReducerUtil.reduceDatesLaws(_ => new MinimumInDaysReducer)
}
