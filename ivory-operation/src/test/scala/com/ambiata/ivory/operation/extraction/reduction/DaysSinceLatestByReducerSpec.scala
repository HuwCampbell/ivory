package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Date
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class DaysSinceLatestByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Calculate the days since latest by a number of facts      $daysSinceLatestBy
"""

  def daysSinceLatestBy = prop((xs: List[(String, Date)]) => {
    val ds = xs.map(td => td._1 -> td._2).sortBy(_._2)
    val dateOffsets = DateOffsets.compact(ds.headOption.map(_._2).getOrElse(Date.minValue),
      ds.lastOption.map(_._2).getOrElse(Date.minValue))
    ReducerUtil.runWithDates(new DaysSinceLatestByReducer(dateOffsets), ds).map.asScala.toMap ====
      ds.groupBy(_._1).mapValues(ds => dateOffsets.untilEnd(ds.map(_._2).sorted.last).value)
  })
}
