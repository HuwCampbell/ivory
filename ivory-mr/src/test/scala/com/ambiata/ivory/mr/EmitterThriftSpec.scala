package com.ambiata.ivory.mr

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.{NullWritable, BytesWritable}
import org.specs2._

class EmitterThriftSpec extends Specification with ScalaCheck { def is = s2"""

  Write and emit thrift value
    $writeAndEmit

"""

  def writeAndEmit = prop((f1: Fact) => {
    val e = TestEmitter[NullWritable, BytesWritable, Array[Byte]]((k, v) => v.copyBytes)
    val bw = Writables.bytesWritable(1024)
    val serialiser = ThriftSerialiser()
    EmitterThrift.writeAndEmit(f1.toNamespacedThrift, NullWritable.get, bw, serialiser, e)
    List(f1) ==== e.emitted.toList.map(serialiser.fromBytesUnsafe(createMutableFact, _))
  })
}
