package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}

class MaximumInDaysReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the maximum in days                                      $maximumInDays
  Maximum in days laws                                               $maximumInDaysLaws
"""

  def maximumInDays = prop((doc: DatesOfCount) => {
    ReducerUtil.reduceDates(doc, new MaximumInDaysReducer) ==== doc.dates.map(_._1).max
  })

  def maximumInDaysLaws =
    ReducerUtil.reduceDatesLaws(_ => new MaximumInDaysReducer)
}
