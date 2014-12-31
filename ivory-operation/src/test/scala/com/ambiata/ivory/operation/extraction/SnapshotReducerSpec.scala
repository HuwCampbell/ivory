package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.lookup.FeatureIdLookup
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable.KeyState
import com.ambiata.ivory.mr._

import org.apache.hadoop.io.BytesWritable
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

  def windowLookupToArray = prop((l: NonEmptyList[(FeatureId, Option[Date])], e: Encoding, m: Mode, x: Date) => {
    val dictionary = Dictionary(l.list.map({ case (fid, date) =>
      Definition.concrete(fid, e, m, None, fid.toString, Nil)
    }))
    val index = dictionary.byFeatureIndex.map({ case (n, d) => d.featureId -> n })
    val lookup = SnapshotJob.windowTable(dictionary, FeatureRanges(l.list.map({ case (f, date) => FeatureRange(f, date.toList, x) })))._2
    val a = SnapshotReducer.windowLookupToArray(lookup)
    seqToResult(l.list.map {
      case (fid, w) => a(index(fid)) ==== w.getOrElse(Date.maxValue).int
    })
  })

  def window = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    MockFactMutator.runKeep(facts.facts) { (bytes, mutator, emitter, out) =>
      SnapshotReducer.reduce(createMutableFact, bytes, mutator, emitter, out, date, isSet = false)
    } ==== (facts.expected -> facts.expected.size)
  }).set(maxSize = 10)

  def windowPriority = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    MockFactMutator.runKeep(facts.factsDupe) { (bytes, mutator, emitter, out) =>
      SnapshotReducer.reduce(createMutableFact, bytes, mutator, emitter, out, date, isSet = false)
    } ==== (facts.expected -> facts.expected.size)
  }).set(maxSize = 10)

  def windowIsSet = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    MockFactMutator.runKeep(facts.factsDupe) { (bytes, mutator, emitter, out) =>
      SnapshotReducer.reduce(createMutableFact, bytes, mutator, emitter, out, date, isSet = true)
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
