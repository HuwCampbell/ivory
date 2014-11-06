package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries.ChordFacts
import com.ambiata.ivory.operation.extraction.squash.SquashArbitraries._
import org.specs2.{ScalaCheck, Specification}

class SquashReducerStateSpec extends Specification with ScalaCheck { def is = s2"""

  Squash a number of facts                                         $squash
  Squash a number of facts for a chord                             $squashChord
  Dump trace for a squash                                          $dump
"""

  def squash = prop((sf: SquashFacts) => {
    val frs = ReducerPool.create(
      SquashJob.concreteGroupToReductions(sf.dict.fid, sf.dict.withExpression(Count).cg, latest = true), (_, r) => r
    )

    MockFactMutator.run(sf.factsSorted) { (bytes, mutator, emitter, out) =>
      val state = new SquashReducerStateSnapshot(sf.date)
      state.reduceAll(createMutableFact, createMutableFact, frs, mutator, bytes, emitter, out)
    } ==== sf.expectedFactsWithCount.sortBy(_.entity)
  })

  def squashChord = prop((cf: ChordFacts) => cf.facts.nonEmpty ==> {
    val pool = ReducerPool.create(
      SquashJob.concreteGroupToReductions(cf.factAndMeta.fact.featureId,
        ConcreteGroup(cf.factAndMeta.meta, List(cf.fid -> VirtualDefinition(cf.factAndMeta.fact.featureId, Query(Count, None), cf.window)))
      , latest = true), (_, r) => r
    )

    val facts = cf.facts.sortBy(fact => (fact.entity, fact.datetime.long))
    MockFactMutator.run(facts) { (bytes, mutator, emitter, out) =>
      val state = new SquashReducerStateChord(cf.chord)
      state.reduceAll(createMutableFact, createMutableFact, pool, mutator, bytes, emitter, out)
    } ==== cf.expectedSquash
  })

  def dump = prop((sf: SquashFacts) => {
    val frs = ReducerPool.create(
      SquashJob.concreteGroupToReductions(sf.dict.fid, sf.dict.withExpression(Count).cg, latest = false), (_, r) => r
    )

    val lines = MockFactMutator.runText(sf.factsSorted) { (bytes, emitter, out) =>
      val state = new SquashReducerStateDump(sf.date, '|', "NA")
      state.reduceAll(createMutableFact, createMutableFact, frs, new FactByteMutator, bytes, emitter, out)
    }
    lines.size ==== sf.facts.size * sf.dict.cg.virtual.size
  })
}
