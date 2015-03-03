package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable.KeyState
import com.ambiata.ivory.mr._
import com.ambiata.poacher.mr._

import org.apache.hadoop.io.BytesWritable
import org.specs2._
import org.specs2.matcher.ThrownExpectations

object SnapshotMapperSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotMapperSpec
-----------

  SnapshotFactsetThriftMapper has correct key/value and count     $factset
  SnapshotIncrementalMapper has correct key/value and count       $incremental

"""
  def disjoint(a: List[Fact], b: List[Fact]): Boolean =
    b.forall(x => !a.exists(_.featureId == x.featureId))

  def factset = prop((fs: List[Fact], priority: Priority) => {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val emitter = newEmitter
    val okCounter = MemoryCounter()

    // Run mapper
    fs.foreach(f => {
      SnapshotFactsetMapper.map(f.toNamespacedThrift, priority, kout, vout, emitter, okCounter, serializer,
        FeatureIdIndex(f.featureId.hashCode))
    })

    assertMapperOutput(emitter, okCounter, fs, priority, serializer)
  })

  def incremental = prop((fs: List[Fact]) => {
    val serializer = ThriftSerialiser()
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val emitter = newEmitter
    val okCounter = MemoryCounter()

    // Run mapper
    fs.foreach(f =>
      SnapshotIncrementalMapper.map(f.toNamespacedThrift, Priority.Max, kout, vout, emitter, okCounter, serializer,
        FeatureIdIndex(f.featureId.hashCode))
    )

    assertMapperOutput(emitter, okCounter, fs, Priority.Max, serializer)
  })

  def assertMapperOutput(emitter: TestEmitter[BytesWritable, BytesWritable, (String, Fact)], okCounter: MemoryCounter,
                         expectedFacts: List[Fact], priority: Priority, serializer: ThriftSerialiser): matcher.MatchResult[Any] = {
    (emitter.emitted.toList, okCounter.counter) ==== (
    (expectedFacts.map(f => (keyBytes(priority)(f), f)), expectedFacts.length))
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
    KeyState.set(f, p, bw, FeatureIdIndex(f.featureId.hashCode))
    new String(bw.copyBytes())
  }
}
