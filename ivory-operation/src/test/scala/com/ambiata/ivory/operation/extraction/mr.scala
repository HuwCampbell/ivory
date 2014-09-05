package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.FeatureIdLookup
import com.ambiata.ivory.operation.extraction.snapshot._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable.KeyState
import com.ambiata.ivory.storage.Arbitraries._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.mr._

import org.apache.hadoop.io.BytesWritable
import org.specs2._
import org.specs2.matcher.ThrownExpectations

import scala.collection.JavaConverters._
import scalaz.NonEmptyList
import scalaz.scalacheck.ScalazArbitrary._

object SnapshotMapperSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotMapperSpec
-----------

  SnapshotFactsetThriftMapper has correct key/value           $e3
  SnapshotIncrementalMapper has correct key/value             $e4

"""

  def e3 = prop((fs: List[Fact], priority: Priority, date: Date, version: FactsetVersion) => {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val tfact = new ThriftFact
    val emitter = TestEmitter()
    val okCounter = TestCounter()
    val skipCounter = TestCounter()

    // Run mapper
    fs.foreach(f => {
      val lookup = new FeatureIdLookup
      lookup.putToIds(f.featureId.toString, f.featureId.hashCode)
      val partition = Partition(f.namespace, f.date)
      val converter = version match {
        case FactsetVersionOne => VersionOneFactConverter(partition)
        case FactsetVersionTwo => VersionTwoFactConverter(partition)
      }
      val bytes = serializer.toBytes(f.toThrift)
      SnapshotFactsetMapper.map(tfact, date, converter, new BytesWritable(bytes), priority, kout, vout, emitter,
        okCounter, skipCounter, serializer, lookup)
    })

    val expected = fs.filter(_.date.isBeforeOrEqual(date))

    assertMapperOutput(emitter, okCounter, skipCounter, expected, fs.length - expected.length, priority, serializer)
  })

  def e4 = prop((fs: List[Fact]) => {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val empty = new NamespacedThriftFact with NamespacedThriftFactDerived
    val emitter = TestEmitter()
    val counter = TestCounter()

    // Run mapper
    fs.foreach(f => {
      val lookup = new FeatureIdLookup
      lookup.putToIds(f.featureId.toString, f.featureId.hashCode)
      val bytes = serializer.toBytes(f.toNamespacedThrift)
      SnapshotIncrementalMapper.map(empty, new BytesWritable(bytes), Priority.Max, kout, vout, emitter, counter,
        serializer, lookup)
    })

    assertMapperOutput(emitter, counter, TestCounter(), fs, 0, Priority.Max, serializer)
  })

  def assertMapperOutput(emitter: TestEmitter, okCounter: TestCounter, skipCounter: TestCounter, expectedFacts: List[Fact], expectedSkip: Int, priority: Priority, serializer: ThriftSerialiser): matcher.MatchResult[Any] =
    emitter.emittedKeys.toList ==== expectedFacts.map(keyBytes(priority)) and
    emitter.emittedVals.toList.map(bytes => deserializeValue(bytes, serializer)) ==== expectedFacts and
    okCounter.i ==== expectedFacts.length and
    skipCounter.i ==== expectedSkip

  def deserializeValue(bytes: Array[Byte], serializer: ThriftSerialiser): Fact =
    serializer.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)

  def keyBytes(p: Priority)(f: Fact): String = {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f, p, bw, f.featureId.hashCode)
    new String(bw.copyBytes())
  }

  case class TestEmitter() extends Emitter[BytesWritable, BytesWritable] {
    import scala.collection.mutable.ListBuffer
    val emittedKeys: ListBuffer[String] = ListBuffer()
    val emittedVals: ListBuffer[Array[Byte]] = ListBuffer()
    def emit(kout: BytesWritable, vout: BytesWritable) {
      emittedKeys += new String(kout.copyBytes)
      emittedVals += vout.copyBytes
      ()
    }
  }

  case class TestCounter() extends Counter {
    var i = 0
    def count(n: Int) {
      i = i + n
    }
  }
}

object SnapshotReducerSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotReducerSpec
-----------

  window lookup to array                                            $windowLookupToArray
  window facts                                                      $window
  window respects priority                                          $windowPriority

"""

  def windowLookupToArray = prop((l: NonEmptyList[(FeatureId, Option[Date])]) => {
    val lookup = SnapshotJob.windowTable(SnapshotWindows(l.list.map((SnapshotWindow.apply _).tupled)))._2
    val a = SnapshotReducer.windowLookupToArray(lookup)
    seqToResult(l.list.zipWithIndex.map {
      case ((fid, w), i) => a(i) ==== w.getOrElse(Date.maxValue).int
    })
  })

  def window = prop((facts: NonEmptyList[Fact], date: Date) => {
    val mutator = new MockFactMutator
    val (oldFacts, newFacts) = facts.list.sortBy(_.date).partition(_.date.int < date.int)
    SnapshotReducer.reduce(createMutableFact, (oldFacts ++ newFacts).asJava.iterator(), mutator, mutator,
      createMutableFact, date)
    mutator.facts.toList ==== (if (newFacts.isEmpty) oldFacts.lastOption.toList else newFacts)
  }).set(maxSize = 10)

  def windowPriority = prop((f: NonEmptyList[Fact], date: Date) => {
    val mutator = new MockFactMutator
    val (oldFacts, newFacts) = f.list.sortBy(_.date).partition(_.date.int < date.int)
    val facts = oldFacts ++ newFacts
    val dupeFacts = facts.zip(facts).flatMap(fs => List(fs._1, fs._2.withEntity("")))
    SnapshotReducer.reduce(createMutableFact, dupeFacts.asJava.iterator(), mutator, mutator,
      createMutableFact, date)
    mutator.facts.toList ==== (if (newFacts.isEmpty) oldFacts.lastOption.toList else newFacts)
  }).set(maxSize = 10)
}
