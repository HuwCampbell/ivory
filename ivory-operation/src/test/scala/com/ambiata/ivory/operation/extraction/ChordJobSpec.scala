package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.MockFactMutator
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import org.specs2._
import scala.collection.JavaConverters._

class ChordJobSpec extends Specification with ScalaCheck { def is = s2"""

  Can extract expected facts     $e1
"""

  def e1 = prop((cf: ChordFact, p: Boolean) => {
    val mutator = new MockFactMutator()
    ChordReducer.reduce(createMutableFact, (if (p) cf.factsWithPriority else cf.facts).iterator.asJava, mutator,
      mutator, createMutableFact, cf.ce.dateArray, new StringBuilder)
    mutator.facts.toList ==== cf.expected
  })
}
