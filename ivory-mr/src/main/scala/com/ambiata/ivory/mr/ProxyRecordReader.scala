package com.ambiata.ivory.mr

import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext

/* This basically works through each of the underlying splits, tracking one underlying RecordReader at a time. */
class ProxyRecordReader[A, B](proxy: ProxyInputSplit, format: InputFormat[A, B], context: TaskAttemptContext) extends RecordReader[SplitKey[A], B] {
  var index = 0
  var underlying: RecordReader[A, B] = format.createRecordReader(proxy.splits(index), context)
  val key = new SplitKey[A](null, null.asInstanceOf[A])

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    underlying.initialize(proxy.splits(index), context)
    key.split = proxy.splits(index)
  }

  override def close: Unit =
    underlying.close

  override def getCurrentKey: SplitKey[A] = {
    key.value = underlying.getCurrentKey
    key
  }

  override def getCurrentValue: B =
    underlying.getCurrentValue

  override def getProgress: Float =
    if (index >= proxy.splits.length)
      1.0.toFloat
    else
      ((index + underlying.getProgress) / proxy.splits.length).toFloat

  override def nextKeyValue: Boolean =
    if (index >= proxy.splits.length)
      false
    else if (underlying.nextKeyValue)
      true
    else {
      nextRecordReader
      nextKeyValue
    }

  def nextRecordReader(): Unit = {
    underlying.close
    index += 1
    if (index < proxy.splits.length) {
      underlying = format.createRecordReader(proxy.splits(index), context)
      underlying.initialize(proxy.splits(index), context)
      key.split = proxy.splits(index)
    }
  }
}
