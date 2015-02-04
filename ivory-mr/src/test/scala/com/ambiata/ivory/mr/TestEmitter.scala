package com.ambiata.ivory.mr

import org.apache.hadoop.io._

case class TestEmitter[K <: Writable, V <: Writable, A](f: (K, V) => A) extends Emitter[K, V] {
  import scala.collection.mutable.ListBuffer
  val emitted: ListBuffer[A] = ListBuffer()
  
  override def emit(kout: K, vout: V): Unit = {
    emitted += f(kout, vout)
    ()
  }
}
