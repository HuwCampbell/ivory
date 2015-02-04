package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries.ChordFacts
import com.ambiata.ivory.operation.extraction.squash.SquashArbitraries._

import com.ambiata.poacher.mr.ThriftSerialiser
import org.specs2.{ScalaCheck, Specification}

class SquashReducerStateSpec extends Specification with ScalaCheck { def is = s2"""

  Squash a number of facts (state)                                 $squashState
  Squash a number of facts (set)                                   $squashSet
  Squash a number of facts for a chord (state)                     $squashChordState
  Squash a number of facts for a chord (set)                       $squashChordSet
  Dump trace for a squash                                          $dump
"""

  def squashState = prop((sfWrongMode: SquashFacts) => {
    val sf = sfWrongMode.withMode(Mode.State)
    squash(sf) ==== sf.expectedFactsWithCountState.sortBy(_.entity)
  })

  def squashSet = prop((sfWrongMode: SquashFacts) => {
    val sf = sfWrongMode.withMode(Mode.Set)
    squash(sf) ==== sf.expectedFactsWithCountSet.sortBy(_.entity)
  })

  def squash(sf: SquashFacts): List[Fact] = {
    val serialiser = ThriftSerialiser()
    val frs = ReducerPool.createTesting(
      SquashJob.concreteGroupToReductions(sf.dict.fid, sf.dict.withExpression(Count).cg, latest = true),
      sf.dict.cg.definition.mode.isSet
    )

    MockFactMutator.run(sf.factsSorted) { (bytes, emitter, out) =>
      val state = new SquashReducerStateSnapshot(sf.date)
      state.reduceAll(createMutableFact, createMutableFact, frs, bytes, emitter, out, serialiser)
    }
  }

  def squashChordState = prop((cf2: ChordFacts) => cf2.facts.nonEmpty ==> {
    val cf = cf2.withMode(Mode.State)
    chord(cf) ==== cf.expectedSquashState
  })

  def squashChordSet = prop((cf2: ChordFacts) => cf2.facts.nonEmpty ==> {
    val cf = cf2.withMode(Mode.Set)
    chord(cf) ==== cf.expectedSquashSet
  })

  def chord(cf: ChordFacts): List[Fact] = {
    val serialiser = ThriftSerialiser()
    val pool = ReducerPool.createTesting(
      SquashJob.concreteGroupToReductions(cf.factAndMeta.fact.featureId,
        ConcreteGroup(cf.factAndMeta.meta, List(cf.fid -> VirtualDefinition(cf.factAndMeta.fact.featureId,
          Query(Count, None), cf.window)))
        , latest = true), cf.factAndMeta.meta.mode.isSet
    )

    val facts = cf.facts.sortBy(fact => (fact.entity, fact.datetime.long))
    MockFactMutator.run(facts) { (bytes, emitter, out) =>
      val state = new SquashReducerStateChord(cf.chord)
      state.reduceAll(createMutableFact, createMutableFact, pool, bytes, emitter, out, serialiser)
    }
  }

  def dump = prop((sf: SquashFacts) => {
    val serialiser = ThriftSerialiser()
    val lines = MockFactMutator.runText(sf.factsSorted) { (bytes, emitter, out) =>
      val frs = ReducerPool.create(
        SquashJob.concreteGroupToReductions(sf.dict.fid, sf.dict.withExpression(Count).cg, latest = true), false,
        SquashDump.wrap('|', "NA", _, _, { line =>
          out.set(line)
          emitter.emit(SquashReducerState.kout, out)
        })
      )
      val state = new SquashReducerStateDump(sf.date)
      state.reduceAll(createMutableFact, createMutableFact, frs, bytes, emitter, out, serialiser)
    }
    lines.map(_.split("\\|")(0)).toSet ==== sf.expected.keySet
  })
}
