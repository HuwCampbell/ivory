package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class LatestReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Keep latest non-tombstone fact                   $latest
  Clear resets the value                           $clear
"""

  def latest = prop((facts: List[Fact]) => {
    val r = new LatestReducer()
    facts.foreach(r.update)
    r.save ==== facts.lastOption.filter(!_.isTombstone).map(_.toThrift.getValue).orNull
  })

  def clear = prop((facts: List[Fact]) => {
    val r = new LatestReducer()
    facts.foreach(r.update)
    r.clear()
    r.save must beNull
  })
}
