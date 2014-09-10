package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.mr._

import org.specs2._
import org.specs2.matcher.ThrownExpectations
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.storage.Arbitraries._
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._

object SnapshotMapperSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotMapperSpec
-----------

  KeyState mutates a BytesWritable with the correct bytes     $e1
  ValueState mutates a PriorityTag with the correct bytes     $e2
  SnapshotFactsetThriftMapper has correct key/value           $e3
  SnapshotIncrementalMapper has correct key/value             $e4

"""

  import SnapshotMapper._

  def e1 = prop((fs: List[Fact]) => {
    val kstate = KeyState(4096)
    val state = Writables.bytesWritable(4096)

    seqToResult(fs.map(f => {
      kstate.set(f, state)
      state.copyBytes must_== keyBytes(f)
    }))
  })

  def e2 = prop((fs: List[Fact], priority: Priority) => {
    val serializer = ThriftSerialiser()
    val vstate = ValueState(priority)
    val state = Writables.bytesWritable(4096)

    seqToResult(fs.map(f => {
      vstate.set(f.toNamespacedThrift, state)
      state.copyBytes must_== serializer.toBytes(new PriorityTag(priority.toShort, ByteBuffer.wrap(serializer.toBytes(f.toNamespacedThrift))))
    }))
  })

  def e3 = prop((fs: List[Fact], priority: Priority, date: Date, version: FactsetVersion) => {
    val serializer = ThriftSerialiser()
    val kstate = KeyState(4096)
    val vstate = ValueState(priority)
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val tfact = new ThriftFact
    val emitter = TestEmitter(kout, vout)
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
      SnapshotFactsetMapper.map(tfact, date, converter, bytes, kstate, vstate, kout, vout, emitter, okCounter, skipCounter, serializer)
    })

    val expected = fs.filter(_.date.isBeforeOrEqual(date))

    assertMapperOutput(emitter, okCounter, skipCounter, expected, fs.length - expected.length, priority, serializer)
  })

  def e4 = prop((fs: List[Fact]) => {
    val serializer = ThriftSerialiser()
    val kstate = KeyState(4096)
    val vstate = ValueState(Priority.Max)
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val empty = new NamespacedThriftFact with NamespacedThriftFactDerived
    val emitter = TestEmitter(kout, vout)
    val counter = TestCounter()

    // Run mapper
    fs.foreach(f => {
      val bytes = serializer.toBytes(f.toNamespacedThrift)
      SnapshotIncrementalMapper.map(empty, bytes, kstate, vstate, kout, vout, emitter, counter, serializer)
    })

    assertMapperOutput(emitter, counter, TestCounter(), fs, 0, Priority.Max, serializer)
  })

  def assertMapperOutput(emitter: TestEmitter, okCounter: TestCounter, skipCounter: TestCounter, expectedFacts: List[Fact], expectedSkip: Int, priority: Priority, serializer: ThriftSerialiser): matcher.MatchResult[Any] =
    emitter.emittedKeys.toList.map(_.toList) ==== expectedFacts.map(keyBytes).map(_.toList) and
    emitter.emittedVals.toList.map(bytes => deserializeValue(bytes, serializer)) ==== expectedFacts.map((priority.toShort, _)) and
    okCounter.i ==== expectedFacts.length and
    skipCounter.i ==== expectedSkip

  def deserializeValue(bytes: Array[Byte], serializer: ThriftSerialiser): (Short, Fact) = {
    val tag = serializer.fromBytesUnsafe(new PriorityTag, bytes)
    (tag.priority, serializer.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, tag.bytes.array()))
  }

  def keyBytes(f: Fact): Array[Byte] =
    f.entity.getBytes ++ f.namespace.name.getBytes ++ f.feature.getBytes

  case class TestEmitter(kout: BytesWritable, vout: BytesWritable) extends Emitter {
    import scala.collection.mutable.ListBuffer
    val emittedKeys: ListBuffer[Array[Byte]] = ListBuffer()
    val emittedVals: ListBuffer[Array[Byte]] = ListBuffer()
    def emit() {
      emittedKeys += kout.copyBytes
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

  case class PriorityDateTimeValue(priority: Priority, datetime: DateTime, value: Value)
  def priorityDateTimeValue(m: ConcreteDefinition): Gen[PriorityDateTimeValue] = for {
    p  <- arbitrary[Priority]
    dt <- arbitrary[DateTime]
    v  <- Gen.frequency(1 -> Gen.const(TombstoneValue()), 99 -> valueOf(m.encoding, m.tombstoneValue))
  } yield PriorityDateTimeValue(p, dt, v)

  case class ReducerFacts(facts: List[(Fact, PriorityTag)])
  implicit def ReducerInputArbitrary: Arbitrary[ReducerFacts] =
    Arbitrary(for {
      (f, m) <- arbitrary[(FeatureId, ConcreteDefinition)]
      e = "T+00001"
      dtvs   <- Gen.choose(1, 10).flatMap(n => Gen.listOfN(n, priorityDateTimeValue(m)))
      pfacts = dtvs.map(dtv => (dtv.priority, Fact.newFactWithNamespaceName(e, f.namespace, f.name, dtv.datetime.date, dtv.datetime.time, dtv.value)))
      reducerFacts = ReducerFacts(pfacts.map({ case (p, f) =>
        (f, new PriorityTag(p.toShort, ByteBuffer.wrap(serializer.toBytes(f.toNamespacedThrift))))
      }))
    } yield reducerFacts)

  def e1 = prop((inputs: List[ReducerFacts]) => {
    val factContainer = new NamespacedThriftFact with NamespacedThriftFactDerived
    val priorityTagContainer = new PriorityTag
    val vout = Writables.bytesWritable(4096)

    seqToResult(inputs.map(in => {
      var actual: BytesWritable = null
      var tombstoneCount = 0
      val emitter = Emitter(() => actual = vout)
      val counter = Counter(n => tombstoneCount = tombstoneCount + n)
  
      val (expectedFact, expectedPriorityTag) = in.facts.maxBy({ case (f, pt) => (f.datetime.long, -pt.priority.toShort) })
      val expectedBytes = if(expectedFact.isTombstone) None else Some(expectedPriorityTag.getBytes)
      val iter = in.facts.map({ case (_, pt) =>
          val bytes = serializer.toBytes(pt)
          val bw = Writables.bytesWritable(4096)
          bw.set(bytes, 0, bytes.length)
          bw
        }).toIterator.asJava
  
      SnapshotReducer.reduce(factContainer, priorityTagContainer, NullWritable.get, vout, iter, emitter, counter, serializer)
      expectedBytes.map(bytes => actual.copyBytes ==== bytes and tombstoneCount ==== 0).getOrElse((actual must beNull) and tombstoneCount ==== 1)
    }))
  })
}
