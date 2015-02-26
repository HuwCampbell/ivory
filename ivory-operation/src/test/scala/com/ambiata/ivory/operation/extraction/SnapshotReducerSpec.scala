package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.mr._

import com.ambiata.poacher.mr.ThriftSerialiser
import org.specs2._
import org.specs2.matcher.ThrownExpectations

import scalaz.NonEmptyList
import scalaz.scalacheck.ScalazArbitrary._

object SnapshotReducerSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotReducerSpec
-----------

  window lookup to array                                            $windowLookupToArray
  window facts                                                      $window
  window respects priority                                          $windowPriority
  window outputs all facts when isSet regardless of priority        $windowIsSet

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

  def window = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    val serialiser = ThriftSerialiser()
    MockFactMutator.runThriftFactKeep(fact.namespace, facts.facts) { (bytes, emitter, kout, vout) =>
      SnapshotReducer.reduce(createMutableFact, bytes, emitter, kout, vout, date, isSet = false, serialiser)
    } ==== (facts.expected -> facts.expected.size)
  }).set(maxSize = 10)

  def windowPriority = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    val serialiser = ThriftSerialiser()
    MockFactMutator.runThriftFactKeep(fact.namespace, facts.factsDupe) { (bytes, emitter, kout, vout) =>
      SnapshotReducer.reduce(createMutableFact, bytes, emitter, kout, vout, date, isSet = false, serialiser)
    } ==== (facts.expected -> facts.expected.size)
  }).set(maxSize = 10)

  def windowIsSet = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    val serialiser = ThriftSerialiser()
    MockFactMutator.runThriftFactKeep(fact.namespace, facts.factsDupe) { (bytes, emitter, kout, vout) =>
      SnapshotReducer.reduce(createMutableFact, bytes, emitter, kout, vout, date, isSet = true, serialiser)
    } ==== (facts.expectedSet -> facts.expectedSet.size)
  }).set(maxSize = 10)

  /** We only care about the DateTime for reducing snapshots, so we reuse the same fact */
  case class SnapshotFacts(dts: NonEmptyList[DateTime], fact: Fact, date: Date) {
    // Make sure we remove distinct times here to avoid confusion later in the dupe test
    val (oldFacts, newFacts) = dts.list.distinct.sortBy(_.long)
      .map(dt => fact.withDate(dt.date).withTime(dt.time))
      .zipWithIndex.map(f => f._1.withValue(IntValue(f._2)))
      .partition(!Window.isFactWithinWindow(date, _))
    def facts: List[Fact] = oldFacts ++ newFacts
    def factsDupe: List[Fact] = dupe(oldFacts) ++ dupe(newFacts)
    lazy val expected: List[Fact] = oldFacts.lastOption.toList ++ newFacts
    lazy val expectedSet: List[Fact] = dupe(oldFacts).lastOption.toList ++ dupe(newFacts)
    def dupe(f: List[Fact]): List[Fact] =
      f.zip(f).flatMap(fs => List(fs._1, fs._2.withValue(IntValue(fs._2.value match { case IntValue(x) => x + 10; case _ => 99 }))))
  }
}
