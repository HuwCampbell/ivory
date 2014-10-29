package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.mr._
import org.apache.hadoop.io._

case class TestEmitter() extends Emitter[BytesWritable, BytesWritable] {
  import scala.collection.mutable.ListBuffer
  val emittedKeys: ListBuffer[String] = ListBuffer()
  val emittedVals: ListBuffer[Array[Byte]] = ListBuffer()
  def emit(kout: BytesWritable, vout: BytesWritable) {
    emittedKeys += new String(kout.copyBytes)
    emittedVals += vout.copyBytes
    ()
  }
}
