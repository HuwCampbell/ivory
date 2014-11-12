package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class IntervalReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Interval mean reducer works with arbitrary facts       $meanInterval

"""

  def meanInterval = prop((facts: List[Fact]) => {
    val ds = facts.map(td => 0 -> td.date).sortBy(_._2)
    val dateOffsets = ReducerUtil.buildDateOffsets(ds)

    val r = new IntervalReducer(dateOffsets, new MeanReducer, ReductionValueDouble)
    facts.sortBy(_.date).foreach(r.update)

    val filteredfacts = facts.filterNot(_.isTombstone).sortBy(_.date)
    val res =
      if (filteredfacts.length > 1)
        filteredfacts.sliding(2, 1).collect { case a :: b :: Nil => DateTimeUtil.toDays(b.date) - DateTimeUtil.toDays(a.date) }.sum.toDouble / (filteredfacts.length - 1)
      else 0.0
    r.save.getD must beCloseTo(res, 4.significantFigures)
  })
}
