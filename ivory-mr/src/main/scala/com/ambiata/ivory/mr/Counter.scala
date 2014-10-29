package com.ambiata.ivory.mr

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.hadoop.mapreduce.{Counter => HCounter}

trait Counter {
  def count(n: Int): Unit
}


object Counter {
  def apply(c: Int => Unit): Counter = new Counter { def count(n: Int) = c(n) }
}

case class MemoryCounter() extends Counter {
  var counter = 0
  def count(n: Int): Unit =
    counter = counter + n
}

case class MrCounter[IK <: Writable, IV <: Writable, OK <: Writable, OV <: Writable](group: String, name: String, context: TaskInputOutputContext[IK, IV, OK, OV]) extends Counter {
  val counter: HCounter = context.getCounter(group, name)

  override def count(n: Int): Unit = {
    counter.increment(n)
  }
}

trait LabelledCounter {
  def count(name: String, n: Int): Unit
}

object LabelledCounter {
  def apply(c: (String, Int) => Unit): LabelledCounter = new LabelledCounter { def count(s: String, n: Int) = c(s, n) }
}

case class MemoryLabelledCounter() extends LabelledCounter {
  val mutable = scala.collection.mutable.Map.empty[String, Int]
  def counters: Map[String, Int] = mutable.toMap

  def count(label: String, n: Int): Unit =
    mutable(label) = (mutable.getOrElse(label, 0) + n)
}


case class MrLabelledCounter[IK <: Writable, IV <: Writable, OK <: Writable, OV <: Writable](group: String, context: TaskInputOutputContext[IK, IV, OK, OV]) extends LabelledCounter {
  override def count(name: String, n: Int): Unit = {
    context.getCounter(group, name).increment(n)
  }
}
