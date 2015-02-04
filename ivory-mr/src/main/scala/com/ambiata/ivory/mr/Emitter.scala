package com.ambiata.ivory.mr

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

/** Abstraction over emitting key/value pairs in an mr job */
trait Emitter[A, B] {
  def emit(key: A, value: B): Unit
}

case class MrContextEmitter[K <: Writable, V <: Writable](context: TaskInputOutputContext[_, _, K, V]) extends Emitter[K, V] {
  override def emit(kout: K, vout: V): Unit =
    context.write(kout, vout)
}

case class MrOutputEmitter[K <: Writable, V <: Writable](name: String, writer: MultipleOutputs[K, V], path: String) extends Emitter[K, V] {
  override def emit(kout: K, vout: V): Unit =
    writer.write(name, kout, vout, path)
}
