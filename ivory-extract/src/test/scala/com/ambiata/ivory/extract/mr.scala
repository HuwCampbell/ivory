package com.ambiata.ivory.extract

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.mr.Writables

import org.specs2._
import org.specs2.matcher.ThrownExpectations
import org.scalacheck._, Arbitrary._, Arbitraries._
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TSerializer, TDeserializer}
import java.nio.ByteBuffer
import java.util.{Iterator => JIterator}

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
      state.copyBytes must_== f.entity.getBytes ++ f.namespace.getBytes ++ f.feature.getBytes
    }))
  })

  def e2 = prop((fs: List[Fact], priority: Priority) => {
    val serializer = new TSerializer(new TCompactProtocol.Factory)
    val vstate = ValueState(priority)
    val state = Writables.bytesWritable(4096)

    seqToResult(fs.map(f => {
      vstate.set(f, state)
      state.copyBytes must_== serializer.serialize(new PriorityTag(priority.toShort, ByteBuffer.wrap(serializer.serialize(f.toNamespacedThrift))))
    }))
  })

  def e3 = prop((fs: List[Fact], priority: Priority) => assertMapperOutput(fs, priority, SnapshotIncrementalMapper.map))

  def e4 = prop((fs: List[Fact]) => assertMapperOutput(fs, Priority.Max, SnapshotIncrementalMapper.map))

  def assertMapperOutput(fs: List[Fact], priority: Priority,
                         map: (NamespacedThriftFact with NamespacedThriftFactDerived, Array[Byte], KeyState, ValueState, BytesWritable, BytesWritable, () => Unit, TDeserializer) => Unit) {

    val serializer = new TSerializer(new TCompactProtocol.Factory)
    val deserializer = new TDeserializer(new TCompactProtocol.Factory)
    val kstate = KeyState(4096)
    val vstate = ValueState(priority)
    val kout = Writables.bytesWritable(4096)
    val vout = Writables.bytesWritable(4096)
    val empty = new NamespacedThriftFact with NamespacedThriftFactDerived
    var actualKout: BytesWritable = null
    var actualVout: BytesWritable = null

    def setActual() {
      actualKout = kout
      actualVout = vout
    }

    seqToResult(fs.map(f => {
      map(empty, serializer.serialize(f.toNamespacedThrift), kstate, vstate, kout, vout, setActual, deserializer)
      (actualKout.copyBytes must_== f.entity.getBytes ++ f.namespace.getBytes ++ f.feature.getBytes) and
      (actualVout.copyBytes must_== serializer.serialize(new PriorityTag(priority.toShort, ByteBuffer.wrap(serializer.serialize(f.toNamespacedThrift)))))
    }))
  }
}

object SnapshotReducerSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

SnapshotReducerSpec
-----------

  crazy mutations work                                              $e1

"""

  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  case class PriorityDateTimeValue(priority: Priority, datetime: DateTime, value: Value)
  def priorityDateTimeValue(m: FeatureMeta): Gen[PriorityDateTimeValue] = for {
    p  <- arbitrary[Priority]
    dt <- arbitrary[DateTime]
    v  <- Gen.frequency(1 -> Gen.const(TombstoneValue()), 99 -> valueOf(m.encoding))
  } yield PriorityDateTimeValue(p, dt, v)

  case class ReducerFacts(facts: List[(Fact, PriorityTag)])
  implicit def ReducerInputArbitrary: Arbitrary[ReducerFacts] =
    Arbitrary(for {
      (f, m) <- Gen.oneOf(TestDictionary.meta.toList)
      e = "T+00001"
      dtvs   <- Gen.nonEmptyListOf(priorityDateTimeValue(m))
      pfacts = dtvs.map(dtv => (dtv.priority, Fact.newFact(e, f.namespace, f.name, dtv.datetime.date, dtv.datetime.time, dtv.value)))
      reducerFacts = ReducerFacts(pfacts.map({ case (p, f) =>
        (f, new PriorityTag(p.toShort, ByteBuffer.wrap(serializer.serialize(f.toNamespacedThrift))))
      }))
    } yield reducerFacts)

  def e1 = prop((input: ReducerFacts) => {
    val factContainer = new NamespacedThriftFact with NamespacedThriftFactDerived
    val priorityTagContainer = new PriorityTag
    val vout = Writables.bytesWritable(4096)

    val (expectedFact, expectedPriorityTag) = input.facts.maxBy({ case (f, pt) => (f.datetime.long, -pt.priority.toShort) })
    val expectedBytes = if(expectedFact.isTombstone) None else Some(expectedPriorityTag.getBytes)
    val iter = input.facts.map({ case (_, pt) =>
        val bytes = serializer.serialize(pt)
        val bw = Writables.bytesWritable(4096)
        bw.set(bytes, 0, bytes.length)
        bw
      }).toIterator.asJava
    var actual: BytesWritable = null

    def setActual() {
      actual = vout
    }

    SnapshotReducer.reduce(factContainer, priorityTagContainer, NullWritable.get, vout, iter, setActual, deserializer)
    expectedBytes.map(bytes => actual.copyBytes must_== bytes).getOrElse(actual must beNull)
  })
}
