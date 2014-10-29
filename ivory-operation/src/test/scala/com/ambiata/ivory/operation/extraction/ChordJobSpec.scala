package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.MockFactMutator
import com.ambiata.ivory.operation.extraction.ChordReducer._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import org.specs2._
import scala.collection.JavaConverters._

class ChordJobSpec extends Specification with ScalaCheck { def is = s2"""

  Can extract expected facts                             $normal
  Can extract expected facts with priority               $priority
  Can extract expected facts with windows                $window
  Can extract expected facts with priority and windows   $prioritywindow
  Can extract expected facts with set                    $set
  Can extract expected facts with windows and set        $windowset

"""
  def N: MockFactMutator => ChordEmitter[Fact] =
    new ChordNormalEmitter(_)

  def W: Array[Int] => MockFactMutator => ChordEmitter[Fact] =
    w => new ChordWindowEmitter(_, w)

  def normal = prop((cf: ChordFact) =>
    reduce(N, cf.facts, cf.expected, cf.ce.dateArray, isSet = false))

  def priority = prop((cf: ChordFact) =>
    reduce(N, cf.factsWithPriority, cf.expected, cf.ce.dateArray, isSet = false))

  def window = prop((cf: ChordFact) =>
    reduce(W(cf.windowDateArray.orNull), cf.facts, cf.expectedWindow, cf.ce.dateArray, isSet = false))

  def prioritywindow = prop((cf: ChordFact) =>
    reduce(W(cf.windowDateArray.orNull), cf.facts, cf.expectedWindow, cf.ce.dateArray, isSet = false))

  def set = prop((cf: ChordFact) =>
    reduce(N, cf.factsWithPriority, cf.expectedSet, cf.ce.dateArray, isSet = true))

  def windowset = prop((cf: ChordFact) =>
    reduce(W(cf.windowDateArray.orNull), cf.factsWithPriority, cf.expectedWindowSet, cf.ce.dateArray, isSet = true))

  def reduce(emitter: MockFactMutator => ChordEmitter[Fact], facts: List[Fact], expected: List[Fact], dateArray: Array[Int], isSet: Boolean) = {
    val mutator = new MockFactMutator()
    ChordReducer.reduce(createMutableFact, facts.iterator.asJava, mutator,
      emitter(mutator), createMutableFact, dateArray, new StringBuilder, isSet)
    mutator.facts.toList ==== expected
  }
}
