package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}

class LatestReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Keep latest non-tombstone fact                   $latest
  Clear resets the value                           $clear
  Keep latest non-tombstone fact from a struct     $latestStruct
  Latest reducer laws                              $latestLaws
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

  def latestStruct = prop((field: String, fact: Fact, values: List[Option[String]]) => {
    val r = ReducerUtil.runWithTombstones(new LatestStructReducer(""), values)
    values.lastOption.flatten.map(s => r.value ==== s and (r.tombstone must beFalse)).getOrElse(r.tombstone must beTrue)
  })

  def latestLaws =
    ReducerUtil.reductionLaws(new LatestReducer)
}
