package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries.ChordFacts
import com.ambiata.ivory.operation.extraction.reduction.Reduction
import com.ambiata.ivory.operation.extraction.squash.SquashArbitraries._
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class SquashReducerStateSpec extends Specification with ScalaCheck { def is = s2"""

  Squash a number of facts                                         $squash
  Squash a number of facts for a chord                             $squashChord
"""

  def squash = prop((sf: SquashFacts) => {
    val frs = SquashReducer.compileAll(
      SquashJob.concreteGroupToReductions(sf.date, sf.dict.fid, sf.dict.withExpression(Count).cg), sf.date, (_, r) => r
    )

    val facts = sf.facts.list.sortBy(fact => (fact.entity, fact.datetime.long))
    val mutator = new MockFactMutator()
    val state = new SquashReducerState(sf.date)
    state.reduceAll(createMutableFact, createMutableFact, frs, mutator, facts.asJava.iterator, mutator, createMutableFact)
    mutator.facts.toList ==== sf.expectedFactsWithCount.sortBy(_.entity)
  })

  def squashChord = prop((cf: ChordFacts) => cf.facts.nonEmpty ==> {
    // This is cheating - we're recompiling the reductions for every entity
    // In future revisions this code should be reusing the logic that:
    // 1. Compiles X reductions where X is the largest number of chords for a single entity
    // 2. Sets the FeatureReduction.date to the the correct Window
    // 3. Resets any start/end dates within those reductions (eg. in DateOffsets and all the DateOffsetsLookups)
    def frs(dates: Array[Int]): Int => List[(FeatureReduction, Reduction)] =
      dates.map { d =>
        val date = Date.unsafeFromInt(d)
        SquashReducer.compileAll(
          SquashJob.concreteGroupToReductions(date, cf.factAndMeta.fact.featureId,
            ConcreteGroup(cf.factAndMeta.meta, List(cf.fid -> VirtualDefinition(cf.factAndMeta.fact.featureId, Query(Count, None), cf.window)))
          ), date, (_, r) => r
        )
      }

    val facts = cf.facts.sortBy(fact => (fact.entity, fact.datetime.long))
    val mutator = new MockFactMutator()
    val state = new SquashChordReducerState(cf.chord)
    state.reduceAll(createMutableFact, createMutableFact, frs, mutator, facts.asJava.iterator, mutator, createMutableFact)
    mutator.facts.toList ==== cf.expectedSquash
  })
}
