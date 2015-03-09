package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.disorder.{NaturalIntSmall, DistinctPair}
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.lookup.FlagLookup
import com.ambiata.ivory.mr.MutableOption
import org.scalacheck._, Prop._
import org.specs2.{ScalaCheck, Specification}

class ModeReducerSpec extends Specification with ScalaCheck { def is = s2"""

Laws
====

  ModeReducerState
    ${laws(ModeReducerState)}

  ModeReducerSet
    ${laws(ModeReducerSet)}

ModeReducerState
================

  Different datetime will always be true
    $stateDifferentFacts

  Same datetime will always be fae
    $stateSameFact

ModeReducerSet
==============

  Will always be false
    $setAcceptAlwaysTrue

Construction
============

  Can lookup a reducer from a mode
    $lookup
"""

  def laws(mr: ModeReducer): Prop = new Properties("ModeReducer laws") {
    property("seed is always equal") =
        mr.seed ?= mr.seed
    property("step is consistent") = forAll((f: Fact) =>
      mr.step(mr.seed, MutableOption.none(mr.seed), f) ?= mr.step(mr.seed, MutableOption.some(mr.seed), f)
    )
  }

  def stateDifferentFacts = prop((f: Fact, d: DistinctPair[DateTime]) => {
    val f1 = f.withDate(d.first.date).withTime(d.first.time)
    val f2 = f.withDate(d.second.date).withTime(d.second.time)
    val s = ModeReducerState.seed
    val o1 = ModeReducerState.step(s, MutableOption.none(s), f1)
    (o1.isSet, ModeReducerState.step(o1.get, o1, f2).isSet) ==== ((true, true))
  })

  def stateSameFact = prop((f1: Fact, f2: Fact) => {
    val f22 = f2.withDate(f1.date).withTime(f1.time)
    val s = ModeReducerState.seed
    val o1 = ModeReducerState.step(s, MutableOption.none(s), f1)
    (o1.isSet, ModeReducerState.step(o1.get, o1, f22).isSet) ==== ((true, false))
  })

  def setAcceptAlwaysTrue = prop((f: Fact) =>
    ModeReducerSet.step(ModeReducerSet.seed, MutableOption.none(ModeReducerSet.seed), f).isSet ==== true
  )

  def lookup = prop((i: NaturalIntSmall, m: Mode) => m.fold(true, true, _ => false) ==> {
    val l = new FlagLookup
    l.putToFlags(i.value, m.fold(false, true, _ => NotImplemented.keyedSet))
    ModeReducer.fromLookup(l)(i.value) ==== ModeReducer.fromMode(m)
  })
}
