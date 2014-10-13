package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}

class CountDaysReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the count of an arbitrary days                         $countDays
"""

  def countDays = prop((doc: DatesOfCount) =>
    ReducerUtil.reduceDates(doc, new CountDaysReducer) ==== doc.dates.size
  )
}
