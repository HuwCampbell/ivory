package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import org.joda.time.{Days => JodaDays}

class DaysSinceReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Can determine the amount of time since the latest date value        $daysSince
  Days since reducer laws                                             $daysSinceLaws

"""

  def daysSince = prop((facts: ValuesWithDate[Option[Date]]) => {
    val answer = ReducerUtil.runWithTombstones(new DaysSinceReducer(facts.offsets), facts.ds.map(_._1.map(_.int)))

    val myDays: Option[Int] = for {
        last <- facts.ds.lastOption
        v    <- last._1
      } yield (JodaDays.daysBetween(v.localDate, last._2.localDate).getDays)

    if (answer.tombstone)
      myDays must beNone
    else
      myDays must beSome(answer.value)
  })

  def daysSinceLaws =
    ReducerUtil.reductionFoldWithDateLaws(offsets => new DaysSinceReducer(offsets))
}
