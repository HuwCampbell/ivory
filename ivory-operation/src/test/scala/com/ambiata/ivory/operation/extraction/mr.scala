package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable.KeyState
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.mr._

import org.specs2._
import org.specs2.matcher.ThrownExpectations
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.storage.Arbitraries._
import org.apache.hadoop.io.{NullWritable, BytesWritable}

import scala.collection.JavaConverters._

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
      val partition = Partition(f.namespace, f.date)
      val converter = version match {
        case FactsetVersionOne => VersionOneFactConverter(partition)
        case FactsetVersionTwo => VersionTwoFactConverter(partition)
      }
      val bytes = serializer.toBytes(f.toThrift)
      SnapshotFactsetMapper.map(tfact, date, converter, new BytesWritable(bytes), priority, kout, vout, emitter,
        okCounter, skipCounter, serializer)
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
      val bytes = serializer.toBytes(f.toNamespacedThrift)
      SnapshotIncrementalMapper.map(empty, new BytesWritable(bytes), Priority.Max, kout, vout, emitter, counter, serializer)
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
    KeyState.set(f, p, bw)
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

  crazy mutations work                                              $e1

"""

  val serializer = ThriftSerialiser()

  def e1 = prop((fact: Fact, fact2: Fact, facts: List[Fact]) => {
    val factContainer = new NamespacedThriftFact with NamespacedThriftFactDerived
    val vout = Writables.bytesWritable(4096)

    var actual: BytesWritable = null
    var tombstoneCount = 0
    val emitter = Emitter[NullWritable, BytesWritable]((kout, vout) => actual = vout)
    val counter = Counter(n => tombstoneCount = tombstoneCount + n)

    // We are grouped by entity, and sorted ascending by datetime/priority
    // So we take the most recent fact with the lowest number priority, in this case 'fact'
    val iter = (facts ++ List(fact, fact2.withDate(fact.date).withTime(fact.time))).map({ f =>
        val bytes = serializer.toBytes(f.toNamespacedThrift)
        val bw = Writables.bytesWritable(4096)
        bw.set(bytes, 0, bytes.length)
        bw
      }).toIterator.asJava

    SnapshotReducer.reduce(factContainer, vout, iter, emitter, counter, serializer)
    if(fact.isTombstone)
      actual must beNull and tombstoneCount ==== 1
    else {
      serializer.fromBytesUnsafe(factContainer, actual.copyBytes())
      factContainer ==== fact.toNamespacedThrift and tombstoneCount ==== 0
    }
  }).set(maxSize = 5)
}
