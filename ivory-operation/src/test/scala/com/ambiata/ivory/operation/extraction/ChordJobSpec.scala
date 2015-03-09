package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.arbitraries.VirtualDictionary
import com.ambiata.ivory.mr.MockFactMutator

import com.ambiata.ivory.operation.extraction.ChordReducer._
import com.ambiata.ivory.operation.extraction.chord.ChordArbitraries._
import com.ambiata.ivory.operation.extraction.mode.ModeReducer
import com.ambiata.ivory.operation.model.ChordModel
import com.ambiata.ivory.storage.lookup.FeatureLookups

import com.ambiata.poacher.mr.ThriftSerialiser
import org.specs2._
import org.specs2.execute.Result
import scalaz._

class ChordJobSpec extends Specification with ScalaCheck { def is = s2"""

  Can extract expected facts with windows                $window
  Can extract expected facts with priority and windows   $prioritywindow
  Can extract expected facts with windows and set        $windowset

  Serialising dictionary windows is symmetrical          $lookupTable
"""

  def window = prop((cf: ChordFact, d: VirtualDictionary) => !cf.facts.isEmpty ==> {
    reduce(cf.ce.entity, d.onConcrete(_.copy(mode = Mode.State)), cf.facts, cf.ce.dateList)
  })

  def prioritywindow = prop((cf: ChordFact, d: VirtualDictionary) => !cf.factsWithPriority.isEmpty ==> {
    reduce(cf.ce.entity, d.onConcrete(_.copy(mode = Mode.State)), cf.factsWithPriority, cf.ce.dateList)
  })

  def windowset = prop((cf: ChordFact, d: VirtualDictionary) => !cf.factsWithPriority.isEmpty ==> {
    reduce(cf.ce.entity, d.onConcrete(_.copy(mode = Mode.Set)), cf.factsWithPriority, cf.ce.dateList)
  })

  def reduce(entity: String, dictionary: VirtualDictionary, facts: List[Fact], dates: List[Date]): Result = {
    val serialiser = ThriftSerialiser()
    val expected = ChordModel.reduceWithDates(NonEmptyList(facts.head, facts.tail: _*), dictionary.concreteGroup, dates)
    val windowStarts = dictionary.vd.window.map(w => dates.map(Window.startingDate(w, _).int).toArray).orNull
    val modeReducer = ModeReducer.fromMode(dictionary.cd.mode, dictionary.concreteGroup.definition.encoding)
      .getOrElse(Crash.error(Crash.CodeGeneration, "Invalid arbitrary"))
    MockFactMutator.run(facts) { (bytes, emitter, out) =>
      ChordReducer.reduce(createMutableFact, bytes, new ChordWindowEmitter(emitter), out, dates.map(_.int).toArray, windowStarts,
        new StringBuilder, modeReducer, serialiser)
    } ==== expected
  }

  def lookupTable = prop((dict: Dictionary) => {
    windowLookupToArray(FeatureLookups.windowTable(dict)) ====
      FeatureLookups.sparseMapToArray(FeatureLookups.maxConcreteWindows(dict).toList, None)
  })
}
