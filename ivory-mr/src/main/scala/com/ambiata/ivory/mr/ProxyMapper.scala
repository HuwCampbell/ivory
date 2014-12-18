package com.ambiata.ivory.mr

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.util.ReflectionUtils

/* A delegating mapper that just manages life-cycle for supporting multiple
   input formats & combined splits at once. */
final class ProxyMapper[A, B, C, D] extends Mapper[SplitKey[A], B, C, D] {
  var mapper: CombinableMapper[A, B, C, D] = null

  override def setup(context: Mapper[SplitKey[A], B, C, D]#Context): Unit = {
    val split = context.getInputSplit.asInstanceOf[ProxyInputSplit]
    mapper = ReflectionUtils.newInstance(split.mapperClass, context.getConfiguration).asInstanceOf[CombinableMapper[A, B, C, D]]
  }

  /* This method carefully maintains the life-cycle of the underlying mapper
     (which must be a CombinableMapper):
       - It should call 'setup' exactly once.
       - It should call 'setupSplit' only when the underlying split changes.
       - It should call 'map' for each record.
       - it should call 'cleanup' exactly once. */
  override def run(context: Mapper[SplitKey[A], B, C, D]#Context): Unit = {
    setup(context)
    val local = context.asInstanceOf[Mapper[A, B, C, D]#Context]
    mapper.setup(local)
    var current: InputSplit = null
    while (context.nextKeyValue()) {
      val k = context.getCurrentKey
      val v = context.getCurrentValue
      if (current == null || !k.split.eq(current)) {
        current = k.split
        mapper.setupSplit(local, k.split)
      }
      mapper.map(k.value, v, local)
    }
    mapper.cleanup(local)
    cleanup(context)
  }
}
