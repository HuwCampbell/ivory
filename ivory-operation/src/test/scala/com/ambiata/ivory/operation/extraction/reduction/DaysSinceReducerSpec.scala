package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}
import org.joda.time.{Days => JodaDays}

class DaysSinceReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Can determine the amount of time since the latest date value        $daysSince

"""

  def daysSince = prop((facts: List[(TestDate, TestDate)]) => {
    val ds = facts.map(td => td._1.d -> td._2.d).sortBy(_._2)
    
    val dateOffsets = DateOffsets.compact(ds.headOption.map(_._2).getOrElse(Date.minValue),
      ds.lastOption.map(_._2).getOrElse(Date.minValue))

    val r = new DaysSinceReducer(dateOffsets)
    ds.map { case (vd, td) => Fact.newFact("e", "n", "f", td, Time(0), DateValue(vd)) }.foreach(r.update)
    Option(r.save) ==== ds.lastOption.map(x => JodaDays.daysBetween(x._1.localDate, x._2.localDate).getDays ).map(ThriftFactValue.i(_))
  })

}
