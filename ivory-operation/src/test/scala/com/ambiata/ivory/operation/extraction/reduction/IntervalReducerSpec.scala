package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class IntervalReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Interval mean reducer works with arbitrary facts       $meanInterval
  Interval reducer laws                                  $intervalLaws

"""

  def meanInterval = prop((facts: FactsWithDate) => {
    val r = new IntervalReducer(facts.offsets, new MeanReducer, ReductionValueDouble)
    facts.ds.foreach(r.update)

    val filteredfacts = facts.ds.filterNot(_.isTombstone)
    val res =
      if (filteredfacts.length > 1)
        filteredfacts.sliding(2, 1).collect { case a :: b :: Nil => DateTimeUtil.toDays(b.date) - DateTimeUtil.toDays(a.date) }.sum.toDouble / (filteredfacts.length - 1)
      else 0.0
    r.save.getD must beCloseTo(res, 4.significantFigures)
  })

  def intervalLaws =
    ReducerUtil.reductionWithDatesLaws(offsets => new IntervalReducer(offsets, new MeanReducer, ReductionValueDouble))
}
