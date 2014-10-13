package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Date
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}

import scala.collection.JavaConverters._

class DaysSinceEarliestByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the days since earliest of a number of facts    $daysSinceEarliestBy
"""

  def daysSinceEarliestBy = prop((xs: List[(String, TestDate)]) => {
    val ds = xs.map(td => td._1 -> td._2.d).sortBy(_._2)
    val dateOffsets = DateOffsets.compact(ds.headOption.map(_._2).getOrElse(Date.minValue),
      ds.lastOption.map(_._2).getOrElse(Date.minValue))
    ReducerUtil.runWithDates(new DaysSinceEarliestByReducer(dateOffsets), ds).map.asScala.toMap ====
      ds.groupBy(_._1).mapValues(ds => dateOffsets.untilEnd(ds.map(_._2).sorted.head).value)
  })
}
