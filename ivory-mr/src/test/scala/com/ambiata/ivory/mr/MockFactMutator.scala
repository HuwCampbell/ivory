package com.ambiata.ivory.mr

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import org.apache.hadoop.io.NullWritable

class MockFactMutator extends MutableStream[MutableFact, Fact] with PipeMutator[Fact, Fact] with Emitter[NullWritable, Fact] {

  val facts = collection.mutable.ListBuffer[Fact]()

  def from(in: Fact, fact: MutableFact): Unit = {
    val serialiser = ThriftSerialiser()
    // Be nice if there was a way in the thrift objects to do this "copy", but it doesn't matter
    serialiser.fromBytesUnsafe(fact, serialiser.toBytes(in.toNamespacedThrift))
    ()
  }

  def pipe(in: Fact, out: Fact): Unit =
    from(in, out.toNamespacedThrift)

  def emit(kout: NullWritable, vout: Fact): Unit = {
    facts += vout
    ()
  }
}
