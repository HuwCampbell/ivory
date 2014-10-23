package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.MockFactMutator
import com.ambiata.ivory.operation.extraction.ChordReducer._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import org.specs2._
import scala.collection.JavaConverters._

class ChordJobSpec extends Specification with ScalaCheck { def is = s2"""

  Can extract expected facts                             $normal
  Can extract expected facts with windows                $window
"""

  def normal = prop((cf: ChordFact, p: Boolean) => {
    val mutator = new MockFactMutator()
    ChordReducer.reduce(createMutableFact, (if (p) cf.factsWithPriority else cf.facts).iterator.asJava, mutator,
      new ChordNormalEmitter(mutator), createMutableFact, cf.ce.dateArray, new StringBuilder)
    mutator.facts.toList ==== cf.expected
  })

  def window = prop((cf: ChordFact, p: Boolean) => {
    val mutator = new MockFactMutator()
    ChordReducer.reduce(createMutableFact, (if (p) cf.factsWithPriority else cf.facts).iterator.asJava, mutator,
      new ChordWindowEmitter(mutator, cf.windowDateArray.orNull), createMutableFact, cf.ce.dateArray, new StringBuilder)
    mutator.facts.toList ==== cf.expectedWindow
  })
}
