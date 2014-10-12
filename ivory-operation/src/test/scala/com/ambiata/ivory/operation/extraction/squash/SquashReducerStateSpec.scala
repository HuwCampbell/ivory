package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.squash.SquashArbitraries._
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class SquashReducerStateSpec extends Specification with ScalaCheck { def is = s2"""

  Squash a number of facts                                         $squash
"""

  def squash = prop((sf: SquashFacts) => {
    val frs = SquashReducer.compileAll(
      SquashJob.concreteGroupToReductions(sf.date, sf.dict.fid, sf.dict.withExpression(Count).cg), sf.date
    )

    val facts = sf.facts.list.sortBy(fact => (fact.entity, fact.datetime.long))
    val mutator = new MockFactMutator()
    val state = new SquashReducerState(sf.date)
    state.reduceAll(createMutableFact, createMutableFact, frs, mutator, facts.asJava.iterator, mutator, createMutableFact)
    mutator.facts.toList ==== sf.expectedFactsWithCount.sortBy(_.entity)
  })
}
