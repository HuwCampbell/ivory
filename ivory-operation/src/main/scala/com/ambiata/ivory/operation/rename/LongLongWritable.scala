package com.ambiata.ivory.operation.rename

import java.io.{DataOutput, DataInput}

import org.apache.hadoop.io.{WritableComparable, WritableComparator}

object LongLongWritable {
  class Comparator extends WritableComparator(classOf[LongLongWritable], true) { }
}

/* When you absolutely, positively have to out-long every writable in the room */
class LongLongWritable(var l1: Long, var l2: Long) extends WritableComparable[LongLongWritable] {

  def this() = this(0, 0)

  def set(l: Long, i: Long): Unit = {
    this.l1 = l
    this.l2 = i
  }

  def readFields(in: DataInput): Unit = {
    l1 = in.readLong
    l2 = in.readLong
  }

  def write(out: DataOutput): Unit = {
    out.writeLong(l1)
    out.writeLong(l2)
  }

  def compareTo(o: LongLongWritable): Int = {
    if (this.l1 < o.l1) -1
    else if (this.l1 == o.l1) {
      if (this.l2 < o.l2) -1 else if (this.l2 == o.l2) 0 else 1
    } else 1
  }

  override def hashCode(): Int =
    31 * (l1 ^ (l1 >>> 32)).asInstanceOf[Int] + (l2 ^ (l2 >>> 32)).asInstanceOf[Int]

  override def equals(o: Any): Boolean =
    o != null && o.isInstanceOf[LongLongWritable] && compareTo(o.asInstanceOf[LongLongWritable]) == 0

  override def toString: String =
    s"$l1:$l2"
}
