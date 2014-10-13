package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._, Arbitraries._
import org.specs2.{ScalaCheck, Specification}

class ProportionByTimeReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the proportion by time of an arbitrary number of facts              $proportionByTime
"""

  def proportionByTime = prop((facts: List[Fact], s: Time, e: Time) => {
    val r = new ProportionByTimeReducer(s, e)
    facts.foreach(r.update)
    val times = facts.filterNot(_.isTombstone).map(_.time).filter(t => s.seconds <= t.seconds && t.seconds <= e.seconds)
    r.save.getD ==== (if (facts.isEmpty) 0 else times.size / facts.size.toDouble)
  })
}
