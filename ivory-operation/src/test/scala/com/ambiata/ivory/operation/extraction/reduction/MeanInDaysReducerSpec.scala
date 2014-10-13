package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Date
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}

class MeanInDaysReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the mean in days         $meanInDays
  Calculate the mean in weeks        $meanInWeeks
"""

  def meanInDays = prop((doc: DatesOfCount) => {
    ReducerUtil.reduceDates(doc, new MeanInDaysReducer) ==== doc.dates.map(_._1).sum / doc.noOfDays
  })

  def meanInWeeks = prop((doc: DatesOfCount) => {
    // We ignore the days not contained in a full week
    val start = doc.dates.lastOption.map(_._2).map(_.localDate.minusDays(doc.noOfWeeksFloor * 7)).map(Date.fromLocalDate).getOrElse(Date.minValue)
    ReducerUtil.reduceDates(doc, new MeanInWeeksReducer) ====
      (if (doc.noOfWeeksFloor != 0) doc.dates.filter(_._2.int >= start.int).map(_._1).sum / doc.noOfWeeksFloor else 0)
  })
}
