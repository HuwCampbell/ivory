package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core.Fact
import org.specs2.{ScalaCheck, Specification}

class ThriftDerserializationBehaviourSpec extends Specification with ScalaCheck { def is = s2"""

  Clearing a thrift object is unnecessary                     $clear
"""

  def clear = prop((f1: Fact, f2: Fact, f3: Fact) => {
    val serialiser = ThriftSerialiser()
    val bytes = serialiser.toBytes(f1.toThrift)
    // We want to show that clearing here is redundant (especially with optional fields)
    // In particular ThriftFact has optional seconds will sometimes not be set
    f2.toThrift.clear()
    // We want to compare deserialisation to one "dirty" (_and_ different) thrift object to a cleared one
    serialiser.fromBytes(f3.toThrift, bytes) ==== serialiser.fromBytes(f2.toThrift, bytes)
  })
}
