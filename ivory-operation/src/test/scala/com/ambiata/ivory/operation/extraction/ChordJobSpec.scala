package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.mr.MockFactMutator

import com.ambiata.ivory.operation.extraction.ChordReducer._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import com.ambiata.ivory.storage.lookup.FeatureLookups

import com.ambiata.poacher.mr.ThriftSerialiser
import org.specs2._
import org.specs2.execute.Result

class ChordJobSpec extends Specification with ScalaCheck { def is = s2"""

  Can extract expected facts with windows                $window
  Can extract expected facts with priority and windows   $prioritywindow
  Can extract expected facts with windows and set        $windowset

  Serialising dictionary windows is symmetrical          $lookupTable
"""

  def window = prop((cf: ChordFact) =>
    reduce(cf.facts, cf.expectedWindow, cf.ce.dateArray, cf.windowDateArray.orNull, isSet = false))

  def prioritywindow = prop((cf: ChordFact) =>
    reduce(cf.facts, cf.expectedWindow, cf.ce.dateArray, cf.windowDateArray.orNull, isSet = false))

  def windowset = prop((cf: ChordFact) =>
    reduce(cf.factsWithPriority, cf.expectedWindowSet, cf.ce.dateArray, cf.windowDateArray.orNull, isSet = true))

  def reduce(facts: List[Fact], expected: List[Fact], dateArray: Array[Int], windowStarts: Array[Int], isSet: Boolean): Result = {
    val serialiser = ThriftSerialiser()
    MockFactMutator.run(facts) { (bytes, emitter, out) =>
      ChordReducer.reduce(createNamespacedFact, bytes, new ChordWindowEmitter(emitter), out, dateArray, windowStarts,
        new StringBuilder, isSet, serialiser)
    } ==== expected
  }

  def lookupTable = prop((dict: Dictionary) => {
    windowLookupToArray(FeatureLookups.windowTable(dict)) ====
      FeatureLookups.sparseMapToArray(FeatureLookups.maxConcreteWindows(dict).toList, None)
  })
}
