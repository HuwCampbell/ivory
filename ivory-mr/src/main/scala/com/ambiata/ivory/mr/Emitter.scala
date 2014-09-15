package com.ambiata.ivory.mr

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.TaskInputOutputContext

/** Abstraction over emitting key/value pairs in an mr job */
trait Emitter[A, B] {
  def emit(key: A, value: B): Unit
}

case class MrEmitter[IK <: Writable, IV <: Writable, OK <: Writable, OV <: Writable]() extends Emitter[OK, OV] {
  var context: TaskInputOutputContext[IK, IV, OK, OV] = null

  override def emit(kout: OK, vout: OV): Unit = {
    context.write(kout, vout)
  }
}
