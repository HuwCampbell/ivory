package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}

class MeanInDaysReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the mean in days         $meanInDays
  Calculate the mean in weeks        $meanInWeeks
  Mean in days laws                  $meanInDaysLaws
  Mean in weeks laws                 $meanInWeeksLaws
"""

  def meanInDays = prop((doc: DatesOfCount) => {
    ReducerUtil.reduceDates(doc, new MeanInDaysReducer) ==== doc.dates.map(_._1).sum.toDouble / doc.noOfDays
  })

  def meanInWeeks = prop((doc: DatesOfCount) => {
    ReducerUtil.reduceDates(doc, new MeanInWeeksReducer) ====
      (if (doc.noOfWeeksFloor != 0) doc.datesWithFullWeeks.map(_._1).sum.toDouble / doc.noOfWeeksFloor else 0)
  })

  def meanInDaysLaws =
    ReducerUtil.reduceDatesLaws(_ => new MeanInDaysReducer)

  def meanInWeeksLaws =
    ReducerUtil.reduceDatesLaws(_ => new MeanInWeeksReducer)
}
