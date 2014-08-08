package com.ambiata.ivory.mr

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.TaskInputOutputContext

/* Abstraction over emitting key/value pairs in an mr job */
trait Emitter {
  def emit(): Unit
}

object Emitter {
  def apply(e: () => Unit): Emitter = new Emitter { def emit(): Unit = e() }
}

case class MrEmitter[IK <: Writable, IV <: Writable, OK <: Writable, OV <: Writable](kout: OK, vout: OV) extends Emitter {
  var context: TaskInputOutputContext[IK, IV, OK, OV] = null

  override def emit(): Unit = {
    context.write(kout, vout)
  }
}
