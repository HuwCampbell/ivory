package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.mundane.bytes.Buffer
import org.apache.hadoop.io.WritableComparator
import org.specs2.{ScalaCheck, Specification}

class ModeKeySpec extends Specification with ScalaCheck { def is = s2"""

  valueBytes must be symmetrical
    $valueBytes
  valueBytes must append bytes that respect the natural equality of the value
    $valueBytesEquality
  fromStruct must return an array for a legal struct
    $fromStruct
  const must always append the provided bytes
    $const
"""

  def valueBytes = prop((pv: PrimitiveValuePair) => {
    val value = Value.toThriftPrimitive(pv.v1)
    ModeKey.byteValue(pv.e)(ModeKey.valueBytes(pv.e)(value, Buffer.empty(255))) ==== value
  })

  def valueBytesEquality = prop((v: PrimitiveValuePair) => v.v1 != v.v2 ==> {
    val b1 = ModeKey.valueBytes(v.e)(Value.toThriftPrimitive(v.v1), Buffer.empty(255))
    val b2 = ModeKey.valueBytes(v.e)(Value.toThriftPrimitive(v.v2), Buffer.empty(255))
    WritableComparator.compareBytes(b1.bytes, b1.offset, b1.length, b2.bytes, b2.offset, b2.length) != 0
  })

  def fromDictionary = prop((d: Dictionary) =>
    ModeKey.fromDictionary(d).length ==== d.definitions.length
  )

  def fromStruct = prop((e: StructEntity, f: Fact) => {
    val modeKey = ModeKey.fromStruct(e.k, e.e).fold(e => Crash.error(Crash.Invariant, e), identity)
    modeKey.append(f.withValue(e.v), Buffer.empty(255)) must not beNull
  })

  def const = prop((f: Fact, a: Array[Byte]) => !a.isEmpty ==> {
    val b = ModeKey.const(a).append(f, Buffer.empty(a.length))
    WritableComparator.compareBytes(a, 0, a.length, b.bytes, b.offset, b.length) ==== 0
  })
}
