package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.operation.extraction.mode.ModeReducer
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.model.ModeHandler

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
    reduce(toFacts(dts, fact), fact, date, Mode.State)
  }).set(maxSize = 10)

  def windowPriority = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    def dupe(f: List[Fact]): List[Fact] =
      f.zip(f).flatMap(fs => List(fs._1, fs._2.withValue(IntValue(fs._2.value match { case IntValue(x) => x + 10; case _ => 99 }))))
    reduce(dupe(toFacts(dts, fact)), fact, date, Mode.State)
  }).set(maxSize = 10)

  def windowIsSet = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    reduce(toFacts(dts, fact), fact, date, Mode.Set)
  }).set(maxSize = 10)

  def reduce(facts: List[Fact], fact: Fact, date: Date, mode: Mode) = {
    val serialiser = ThriftSerialiser()
    val expected = ModeHandler.get(mode).reduce(NonEmptyList(facts.head, facts.tail: _*), date)
    MockFactMutator.runThriftFactKeep(fact.namespace, facts) { (bytes, emitter, kout, vout) =>
      SnapshotReducer.reduce(createMutableFact, bytes, emitter, kout, vout, date, ModeReducer.fromMode(mode), serialiser)
    } ==== (expected -> expected.size)
  }

  def toFacts(dts: NonEmptyList[DateTime], fact: Fact): List[Fact] =
    dts.list.distinct.sortBy(_.long)
      .map(dt => fact.withDate(dt.date).withTime(dt.time))
}
