package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core.Fact
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TCompactProtocol
import org.specs2.{ScalaCheck, Specification}

class ThriftDerserializationBehaviourSpec extends Specification with ScalaCheck { def is = s2"""

  Clearing a thrift object is crucial!!!                            $clearUnsafe
  Clearing a thrift object is unnecessary for ThriftSerialiser      $clear
  Thrift facts with optional seconds is smaller than mandatory      $size
"""

  def clear = prop((f1: Fact, f2: Fact, f3: Fact, t: Int) => {
    val serialiser = ThriftSerialiser()
    f1.toThrift.unsetSeconds()
    // We want to show that clearing here (for our own ThriftSerialiser) is redundant (especially with optional fields)
    // In particular ThriftFact has optional seconds will sometimes not be set
    f2.toThrift.clear()
    f3.toThrift.setSeconds(t)
    val bytes = serialiser.toBytes(f1.toThrift)
    // We want to compare deserialisation to one "dirty" (_and_ different) thrift object to a cleared one
    serialiser.fromBytesUnsafe(f2.toThrift, bytes) ==== serialiser.fromBytesUnsafe(f3.toThrift, bytes)
  }).set(minTestsOk = 1)

  def clearUnsafe = prop((f1: Fact, f2: Fact, f3: Fact, t: Int) => {
    val serialiser = new TSerializer(new TCompactProtocol.Factory)
    val deserialiser = new TDeserializerCopy(new TCompactProtocol.Factory)
    f1.toThrift.unsetSeconds()
    f2.toThrift.clear()
    f3.toThrift.setSeconds(t)
    val bytes = serialiser.serialize(f1.toThrift)
    deserialiser.deserialize(f2.toThrift, bytes)
    deserialiser.deserialize(f3.toThrift, bytes)
    // Normally we would expect these to be the same, but we have set an optional field that was blank in f1
    f2.toThrift !=== f3.toThrift
  }).set(minTestsOk = 1)

  def size = prop((fact: Fact, seconds: Int) => {
    val serialiser = ThriftSerialiser()
    val f2 = new ThriftFactTest(fact.entity, fact.feature, fact.toThrift.getValue, fact.time.seconds)
    serialiser.toBytes(fact.toThrift).length must lessThanOrEqualTo(serialiser.toBytes(f2).length)
  })
}
