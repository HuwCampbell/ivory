package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.mr.{Emitter, MockFactMutator}
import com.ambiata.ivory.operation.extraction.ChordReducer._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import com.ambiata.ivory.storage.lookup.FeatureLookups
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.specs2._
import org.specs2.execute.Result

class ChordJobSpec extends Specification with ScalaCheck { def is = s2"""

  Can extract expected facts                             $normal
  Can extract expected facts with priority               $priority
  Can extract expected facts with windows                $window
  Can extract expected facts with priority and windows   $prioritywindow
  Can extract expected facts with set                    $set
  Can extract expected facts with windows and set        $windowset

  Serialising dictionary windows is symmetrical          $lookupTable
"""
  def N: Emitter[NullWritable, BytesWritable] => ChordEmitter =
    new ChordNormalEmitter(_)

  def W: Array[Int] => Emitter[NullWritable, BytesWritable] => ChordEmitter =
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

  def reduce(chordEmitter: Emitter[NullWritable, BytesWritable] => ChordEmitter, facts: List[Fact], expected: List[Fact], dateArray: Array[Int], isSet: Boolean): Result = {
    MockFactMutator.run(facts) { (bytes, mutator, emitter, out) =>
      ChordReducer.reduce(createMutableFact, bytes, mutator, chordEmitter(emitter), out, dateArray, new StringBuilder, isSet)
    } ==== expected
  }

  def lookupTable = prop((dict: Dictionary) => {
    windowLookupToArray(FeatureLookups.windowTable(dict)) ====
      FeatureLookups.sparseMapToArray(FeatureLookups.maxConcreteWindows(dict).toList, None)
  })
}
