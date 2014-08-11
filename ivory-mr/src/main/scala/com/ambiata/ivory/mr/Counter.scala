package com.ambiata.ivory.mr

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.TaskInputOutputContext

/* This is used to abstract over mr specific counters */
trait Counter {
  def count(n: Int): Unit
}

object Counter {
  def apply(c: Int => Unit): Counter = new Counter { def count(n: Int) = c(n) }
}

case class MrCounter[IK <: Writable, IV <: Writable, OK <: Writable, OV <: Writable](group: String, name: String) extends Counter {
  var context: TaskInputOutputContext[IK, IV, OK, OV] = null

  override def count(n: Int): Unit = {
    context.getCounter(group, name).increment(n)
  }
}
