package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.disorder.{NaturalIntSmall, DistinctPair}
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.gen.GenFact
import com.ambiata.ivory.mr.MutableOption
import org.apache.hadoop.io.WritableComparator
import org.scalacheck._, Prop._
import org.specs2.{ScalaCheck, Specification}

class ModeReducerSpec extends Specification with ScalaCheck { def is = s2"""

Laws
====

  ModeReducerState
    ${lawsSimple(ModeReducerState)}

  ModeReducerSet
    ${lawsSimple(ModeReducerSet)}

  ModeReducerKeyedSet
    $keyedSetLaws

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

ModeReducerKeyedSet
===================

  Will not accept when two values are equal
    $keyedSetEqual

  Will accept when two values are not equal
    $keyedSetNotEqual

Construction
============

  Can lookup a reducer from a mode
    $lookup
"""

  def keyedSetLaws = prop((se: SparseEntities) =>
    laws(
      new ModeReducerKeyedSet(ModeKey.fromDefinition(se.meta.mode, se.meta.encoding)
        .getOrElse(Crash.error(Crash.Invariant, "Invalid mode and encoding"))),
      Gen.const(se.fact)
    ) {
      (b1, b2) =>
        WritableComparator.compareBytes(
          b1.buffer1.bytes, b1.buffer1.offset, b1.buffer1.length,
          b2.buffer1.bytes, b2.buffer1.offset, b2.buffer1.length
        ) == 0
    }
  )

  def lawsSimple(mr: ModeReducer): Prop =
    laws(mr, GenFact.fact)(_ ?= _)

  def laws(mr: ModeReducer, gen: Gen[Fact])(compare: (mr.X, mr.X) => Prop): Prop = new Properties("ModeReducer laws") {
    property("seed is always equal") =
      compare(mr.seed, mr.seed)
    property("step is consistent") = forAll(gen)((f: Fact) => {
      val s1 = mr.step(mr.seed, MutableOption.none(mr.seed), f)
      val s2 = mr.step(mr.seed, MutableOption.some(mr.seed), f)
      if (s1.isSet && s2.isSet) compare(s1.get, s2.get) else s1.isSet ?= s2.isSet
    })
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

  def keyedSetEqual = prop((se: StructEntity, f1: Fact, f2: Fact) => {
    val mr = new ModeReducerKeyedSet(ModeKey.fromStruct(se.k, se.e).fold(sys.error, identity))
    val o1 = mr.step(mr.seed, MutableOption.none(mr.seed), f1.withValue(se.v).withDateTime(f1.datetime))
    mr.step(o1.get, o1, f2.withValue(se.v).withDateTime(f1.datetime)).isSet must beFalse
  })

  def keyedSetNotEqual = prop((k: String, f: PrimitiveValuePair, fact: Fact) => f.v1 != f.v2 ==> {
    val mr = new ModeReducerKeyedSet(ModeKey.fromStruct(k, StructEncoding(Map(k -> StructEncodedValue.mandatory(f.e)))).fold(sys.error, identity))
    val o1 = mr.step(mr.seed, MutableOption.none(mr.seed), fact.withValue(StructValue(Map(k -> f.v1))))
    mr.step(o1.get, o1, fact.withValue(StructValue(Map(k -> f.v2)))).isSet must beTrue
  })

  def lookup = prop((i: NaturalIntSmall, cd: ConcreteDefinition) =>
    ModeReducer.fromLookup(Map(i.value -> (cd.mode -> cd.encoding)))(i.value).getClass.getName ====
      ModeReducer.fromMode(cd.mode, cd.encoding).fold(sys.error, identity).getClass.getName
  )
}
