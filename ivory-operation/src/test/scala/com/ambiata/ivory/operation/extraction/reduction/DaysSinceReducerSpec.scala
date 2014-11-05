package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import org.joda.time.{Days => JodaDays}

class DaysSinceReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Can determine the amount of time since the latest date value        $daysSince

"""

  def daysSince = prop((facts: List[(Option[Date], Date)]) => {
    val ds = facts.map(td => td._1 -> td._2).sortBy(_._2)

    val dateOffsets = DateOffsets.compact(ds.headOption.map(_._2).getOrElse(Date.minValue),
      ds.lastOption.map(_._2).getOrElse(Date.minValue))

    val answer = ReducerUtil.runWithTombstones(new DaysSinceReducer(dateOffsets), ds.map(_._1.map(_.int)))

    val myDays: Option[Int] = for {
        last <- ds.lastOption
        v    <- last._1
      } yield (JodaDays.daysBetween(v.localDate, last._2.localDate).getDays)

    if (answer.tombstone)
      myDays must beNone
    else
      myDays must beSome(answer.value)
  })
}
