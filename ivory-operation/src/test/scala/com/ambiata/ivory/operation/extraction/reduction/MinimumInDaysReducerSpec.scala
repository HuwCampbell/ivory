package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}

class MinimumInDaysReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the minimum in days                                      $minimumInDays
"""

  def minimumInDays = prop((doc: DatesOfCount) => {
    ReducerUtil.reduceDates(doc, new MinimumInDaysReducer) ==== doc.dates.map(_._1).min
  })
}
