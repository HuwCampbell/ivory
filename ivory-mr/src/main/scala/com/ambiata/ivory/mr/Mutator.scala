package com.ambiata.ivory.mr

import com.ambiata.poacher.mr.ThriftSerialiser
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import org.apache.hadoop.io.BytesWritable

class ThriftByteMutator[T](implicit ev: T <:< ThriftLike) {

  val serializer = ThriftSerialiser()

  def from(in: BytesWritable, thrift: T): Unit = {
    serializer.fromBytesViewUnsafe(thrift, in.getBytes, 0, in.getLength)
    ()
  }

  def mutate(in: T, vout: BytesWritable): Unit = {
    // It's unfortunate we can't re-use the byte array here too :(
    val bytes = serializer.toBytes(in)
    vout.set(bytes, 0, bytes.length)
  }

  def pipe(in: BytesWritable, vout: BytesWritable): Unit =
    // We are saving a minor step of serialising the (unchanged) thrift fact
    vout.set(in.getBytes, 0, in.getLength)
}

/**
 * The most common mutation case which is that we are mutating a single thrift object.
 */
class FactByteMutator extends ThriftByteMutator[MutableFact]
