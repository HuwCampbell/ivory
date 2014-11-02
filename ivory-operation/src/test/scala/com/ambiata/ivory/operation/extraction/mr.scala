package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.FeatureIdLookup
import com.ambiata.ivory.operation.extraction.snapshot._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable.KeyState
import com.ambiata.ivory.mr._
import com.ambiata.ivory.storage.Arbitraries._
import com.ambiata.ivory.storage.fact._
import com.ambiata.poacher.mr._

import org.apache.hadoop.io.BytesWritable
import org.specs2._
import org.specs2.matcher.ThrownExpectations

import scalaz.NonEmptyList
import scalaz.scalacheck.ScalazArbitrary._

object SnapshotMapperSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotMapperSpec
-----------

  SnapshotFactsetThriftMapper has correct key/value and count     $factset
  SnapshotIncrementalMapper has correct key/value and count       $incremental

"""
  def disjoint(a: List[Fact], b: List[Fact]): Boolean =
    b.forall(x => !a.exists(_.featureId == x.featureId))

  def factset = prop((fs: List[Fact], dropped: List[Fact], priority: Priority, date: Date, version: FactsetVersion) => disjoint(fs, dropped) ==> {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val tfact = new ThriftFact
    val emitter = TestEmitter()
    val okCounter = MemoryCounter()
    val skipCounter = MemoryCounter()
    val dropCounter = MemoryLabelledCounter()
    val lookup = new FeatureIdLookup(new java.util.HashMap[String, Integer])
    fs.foreach(f => lookup.putToIds(f.featureId.toString, f.featureId.hashCode))

    // Run mapper
    (fs ++ dropped).foreach(f => {
      val partition = Partition(f.namespace, f.date)
      val converter = version match {
        case FactsetVersionOne => VersionOneFactConverter(partition)
        case FactsetVersionTwo => VersionTwoFactConverter(partition)
      }
      val bytes = serializer.toBytes(f.toThrift)
      SnapshotFactsetMapper.map(tfact, date, converter, new BytesWritable(bytes), priority, kout, vout, emitter,
        okCounter, skipCounter, dropCounter, serializer, lookup)
    })

    val expected = fs.filter(_.date.isBeforeOrEqual(date))

    assertMapperOutput(emitter, okCounter, skipCounter, dropCounter, expected, fs.length - expected.length, dropped.length, priority, serializer)
  })

  def incremental = prop((fs: List[Fact], dropped: List[Fact]) => disjoint(fs, dropped) ==> {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val empty = new NamespacedThriftFact with NamespacedThriftFactDerived
    val emitter = TestEmitter()
    val okCounter = MemoryCounter()
    val dropCounter = MemoryLabelledCounter()
    val lookup = new FeatureIdLookup(new java.util.HashMap[String, Integer])
    fs.foreach(f => lookup.putToIds(f.featureId.toString, f.featureId.hashCode))

    // Run mapper
    (fs ++ dropped).foreach(f => {
      val bytes = serializer.toBytes(f.toNamespacedThrift)
      SnapshotIncrementalMapper.map(empty, new BytesWritable(bytes), Priority.Max, kout, vout,
                                    emitter, okCounter, dropCounter, serializer, lookup)
    })

    assertMapperOutput(emitter, okCounter, MemoryCounter(), dropCounter, fs, 0, dropped.length, Priority.Max, serializer)
  })

  def assertMapperOutput(emitter: TestEmitter, okCounter: MemoryCounter, skipCounter: MemoryCounter, dropCounter: MemoryLabelledCounter, expectedFacts: List[Fact], expectedSkip: Int, expectedDropped: Int, priority: Priority, serializer: ThriftSerialiser): matcher.MatchResult[Any] = {
    emitter.emittedKeys.toList ==== expectedFacts.map(keyBytes(priority)) and
    emitter.emittedVals.toList.map(bytes => deserializeValue(bytes, serializer)) ==== expectedFacts and
    okCounter.counter ==== expectedFacts.length and
    skipCounter.counter ==== expectedSkip and
    dropCounter.counters.values.sum ==== expectedDropped
  }

  def deserializeValue(bytes: Array[Byte], serializer: ThriftSerialiser): Fact =
    serializer.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)

  def keyBytes(p: Priority)(f: Fact): String = {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f, p, bw, f.featureId.hashCode)
    new String(bw.copyBytes())
  }
}

object SnapshotReducerSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotReducerSpec
-----------

  window lookup to array                                            $windowLookupToArray
  window facts                                                      $window
  window respects priority                                          $windowPriority
  window outputs all facts when isSet reguardless of priority       $windowIsSet

"""

  def windowLookupToArray = prop((l: NonEmptyList[(FeatureId, Option[Date])], e: Encoding, m: Mode) => {
    val dictionary = Dictionary(l.list.map({ case (fid, date) =>
      Definition.concrete(fid, e, m, None, fid.toString, Nil)
    }))
    val index = dictionary.byFeatureIndex.map({ case (n, d) => d.featureId -> n })
    val lookup = SnapshotJob.windowTable(dictionary, SnapshotWindows(l.list.map((SnapshotWindow.apply _).tupled)))._2
    val a = SnapshotReducer.windowLookupToArray(lookup)
    seqToResult(l.list.map {
      case (fid, w) => a(index(fid)) ==== w.getOrElse(Date.maxValue).int
    })
  })

  def window = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    MockFactMutator.run(facts.facts) { (bytes, mutator, emitter, out) =>
      SnapshotReducer.reduce(createMutableFact, bytes, mutator, emitter, out, date, isSet = false)
    } ==== facts.expected
  }).set(maxSize = 10)

  def windowPriority = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    MockFactMutator.run(facts.factsDupe) { (bytes, mutator, emitter, out) =>
      SnapshotReducer.reduce(createMutableFact, bytes, mutator, emitter, out, date, isSet = false)
    } ==== facts.expected
  }).set(maxSize = 10)

  def windowIsSet = prop((dts: NonEmptyList[DateTime], fact: Fact, date: Date) => {
    val facts = SnapshotFacts(dts, fact, date)
    MockFactMutator.run(facts.factsDupe) { (bytes, mutator, emitter, out) =>
      SnapshotReducer.reduce(createMutableFact, bytes, mutator, emitter, out, date, isSet = true)
    } ==== facts.expectedSet
  }).set(maxSize = 10)

  /** We only care about the DateTime for reducing snapshots, so we reuse the same fact */
  case class SnapshotFacts(dts: NonEmptyList[DateTime], fact: Fact, date: Date) {
    // Make sure we remove distinct times here to avoid confusion later in the dupe test
    val (oldFacts, newFacts) = dts.list.distinct.sortBy(_.long)
      .map(dt => fact.withDate(dt.date).withTime(dt.time))
      .zipWithIndex.map(f => f._1.withValue(IntValue(f._2)))
      .partition(_.date.int < date.int)
    def facts: List[Fact] = oldFacts ++ newFacts
    def factsDupe: List[Fact] = dupe(oldFacts) ++ dupe(newFacts)
    def expected: List[Fact] = oldFacts.lastOption.toList ++ newFacts
    def expectedSet: List[Fact] = dupe(oldFacts).lastOption.toList ++ dupe(newFacts)
    def dupe(f: List[Fact]): List[Fact] =
      f.zip(f).flatMap(fs => List(fs._1, fs._2.withValue(IntValue(fs._2.value match { case IntValue(x) => x + 10; case _ => 99 }))))
  }
}
