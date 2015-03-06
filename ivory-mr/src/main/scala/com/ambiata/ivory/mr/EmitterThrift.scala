package com.ambiata.ivory.mr

import com.ambiata.poacher.mr.ThriftSerialiser
import com.nicta.scoobi.io.thrift._
import org.apache.hadoop.io.BytesWritable

object EmitterThrift {

  def writeAndEmit[K](thrift: ThriftLike, kout: K, vout: BytesWritable, serializer: ThriftSerialiser,
                      emitter: Emitter[K, BytesWritable]): Unit = {
    // TODO Ideally we write this directly to the output byte array (and handle resizing)
    val bytes = serializer.toBytes(thrift)
    vout.set(bytes, 0, bytes.length)
    emitter.emit(kout, vout)
  }
}
