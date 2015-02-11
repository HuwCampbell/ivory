package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.FeatureIdLookup
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable.KeyState
import com.ambiata.ivory.mr._
import com.ambiata.ivory.storage.fact._
import com.ambiata.poacher.mr._

import org.apache.hadoop.io.{BytesWritable, IntWritable, NullWritable}
import org.specs2._
import org.specs2.matcher.ThrownExpectations

import scalaz.NonEmptyList

object SnapshotMapperSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotMapperSpec
-----------

  SnapshotFactsetThriftMapper has correct key/value and count     $factset
  SnapshotIncrementalMapper has correct key/value and count       $incremental

"""
  def disjoint(a: List[Fact], b: List[Fact]): Boolean =
    b.forall(x => !a.exists(_.featureId == x.featureId))

  def factset = prop((fs: List[Fact], dropped: List[Fact], priority: Priority, date: Date, version: FactsetFormat) => disjoint(fs, dropped) ==> {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val fact: NamespacedFact = createNamespacedFact
    val emitter = newEmitter
    val okCounter = MemoryCounter()
    val skipCounter = MemoryCounter()
    val dropCounter = MemoryCounter()
    val lookup = new FeatureIdLookup(new java.util.HashMap[String, Integer])
    fs.foreach(f => lookup.putToIds(f.featureId.toString, f.featureId.hashCode))

    // Run mapper
    (fs ++ dropped).foreach(f => {
      val partition = Partition(f.namespace, f.date)
      val converter = version match {
        case FactsetFormat.V1 => PartitionFactConverter(partition)
        case FactsetFormat.V2 => PartitionFactConverter(partition)
      }
      val bytes = serializer.toBytes(f.toThrift)
      SnapshotFactsetMapper.map(fact, date, converter, NullWritable.get, new BytesWritable(bytes), priority, kout, vout, emitter,
        okCounter, skipCounter, dropCounter, serializer, lookup)
    })

    val expected = fs.filter(_.date.isBeforeOrEqual(date))

    assertMapperOutput(emitter, okCounter, skipCounter, dropCounter, expected, fs.length - expected.length, dropped.length, priority, serializer)
  })

  def incremental = prop((fs: List[Fact], dropped: List[Fact], format: SnapshotFormat) => disjoint(fs, dropped) ==> {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val empty: NamespacedFact = createNamespacedFact
    val emitter = newEmitter
    val okCounter = MemoryCounter()
    val dropCounter = MemoryCounter()
    val lookup = new FeatureIdLookup(new java.util.HashMap[String, Integer])
    fs.foreach(f => lookup.putToIds(f.featureId.toString, f.featureId.hashCode))

    // Run mapper
    (fs ++ dropped).foreach(f => format match {
      case SnapshotFormat.V1 =>
        SnapshotIncrementalMapper.map(empty, NullWritable.get, new BytesWritable(serializer.toBytes(f.toNamespacedThrift)),
                                      Priority.Max, kout, vout, emitter, okCounter, dropCounter, serializer, lookup, NamespacedFactConverter())
      case SnapshotFormat.V2 =>
        SnapshotIncrementalMapper.map(empty, new IntWritable(f.date.int), new BytesWritable(serializer.toBytes(f.toThrift)),
                                      Priority.Max, kout, vout, emitter, okCounter, dropCounter, serializer, lookup, NamespaceDateFactConverter(f.namespace))
    })

    assertMapperOutput(emitter, okCounter, MemoryCounter(), dropCounter, fs, 0, dropped.length, Priority.Max, serializer)
  })

  def assertMapperOutput(emitter: TestEmitter[BytesWritable, BytesWritable, (String, Fact)], okCounter: MemoryCounter, skipCounter: MemoryCounter, dropCounter: MemoryCounter, expectedFacts: List[Fact], expectedSkip: Int, expectedDropped: Int, priority: Priority, serializer: ThriftSerialiser): matcher.MatchResult[Any] = {
    (emitter.emitted.toList, okCounter.counter, skipCounter.counter, dropCounter.counter) ==== (
    (expectedFacts.map(f => (keyBytes(priority)(f), f)), expectedFacts.length, expectedSkip, expectedDropped))
  }

  def newEmitter: TestEmitter[BytesWritable, BytesWritable, (String, Fact)] = {
    val serializer = ThriftSerialiser()
    TestEmitter((key, value) => {
      (new String(key.copyBytes), deserializeValue(value.copyBytes, serializer))
    })
  }

  def deserializeValue(bytes: Array[Byte], serializer: ThriftSerialiser): Fact =
    serializer.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)

  def keyBytes(p: Priority)(f: Fact): String = {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f, p, bw, f.featureId.hashCode)
    new String(bw.copyBytes())
  }
}
