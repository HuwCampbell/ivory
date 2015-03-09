package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.arbitraries.{FactsWithStatePriority, SparseEntities}
import com.ambiata.ivory.operation.extraction.mode._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.model.{FactModel, ModeHandler}

import com.ambiata.poacher.mr.ThriftSerialiser
import org.specs2._
import org.specs2.matcher.ThrownExpectations

import scalaz._
import scalaz.scalacheck.ScalazArbitrary._

object SnapshotReducerSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotReducerSpec
-----------

  window lookup to array                                            $windowLookupToArray
  window facts                                                      $window
  window outputs all facts when isSet regardless of priority        $windowIsSet
  window facts (keyed set)                                          $windowKeySet

"""

  def windowLookupToArray = prop((l2: NonEmptyList[(FeatureId, Option[Date])], e: Encoding, m: Mode, x: Date) => {
    // Remove duplicate featureIds
    val l = l2.list.toMap.toList
    val dictionary = Dictionary(l.map({ case (fid, date) =>
      Definition.concrete(fid, e, m, None, fid.toString, Nil)
    }))
    val index = dictionary.byFeatureIndex.map({ case (n, d) => d.featureId -> n })
    val lookup = SnapshotJob.windowTable(dictionary, Ranges(l.map({ case (f, date) => Range(f, date.toList, x) })))._2
    val a = SnapshotReducer.windowLookupToArray(lookup)
    seqToResult(l.map {
      case (fid, w) => a(index(fid)) ==== w.getOrElse(Date.maxValue).int
    })
  })

  def window = prop((facts: FactsWithStatePriority, fact: SparseEntities, date: Date) => {
    reduce(toFacts(facts, fact.fact), fact.onDefinition(_.copy(mode = Mode.State)), date)
  }).set(maxSize = 10)

  def windowIsSet = prop((facts: FactsWithStatePriority, fact: SparseEntities, date: Date) => {
    reduce(toFacts(facts, fact.fact), fact.onDefinition(_.copy(mode = Mode.Set)), date)
  }).set(maxSize = 10)

  def windowKeySet = prop((facts: FactsWithKeyedSetPriority, fact: SparseEntities) => {
    val factsP = FactModel.factsPriority(facts.factsets(fact.fact)).map(_.value)
    val date = facts.dates.max
    reduce(factsP, fact.onDefinition(facts.definition), date)
  }).set(maxSize = 10)

  def reduce(facts: List[Fact], se: SparseEntities, date: Date) = {
    val serialiser = ThriftSerialiser()
    val expected = ModeHandler.get(se.meta.mode).reduce(NonEmptyList(facts.head, facts.tail: _*), date)
    val modeReducer = ModeReducer.fromMode(se.meta.mode, se.meta.encoding)
      .fold(e => Crash.error(Crash.CodeGeneration, s"Incorrect arbitrary for mode/encoding: $e"), identity)
    MockFactMutator.runThriftFactKeep(se.fact.namespace, facts) { (bytes, emitter, kout, vout) =>
      SnapshotReducer.reduce(createMutableFact, bytes, emitter, kout, vout, date, modeReducer, serialiser)
    } ==== (expected -> expected.size)
  }

  def toFacts(facts: FactsWithStatePriority, fact: Fact): List[Fact] =
    FactModel.factsPriority(facts.factsets(fact)).map(_.value).sortBy(_.datetime.long)
}
