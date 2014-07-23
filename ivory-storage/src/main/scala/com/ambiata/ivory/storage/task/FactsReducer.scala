package com.ambiata.ivory.storage.task

import com.ambiata.ivory.lookup.NamespaceLookup
import com.ambiata.ivory.mr.{ThriftCache, MrContext}
import com.ambiata.ivory.storage.lookup.ReducerLookups
import org.apache.hadoop.io.{NullWritable, BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

/**
 * Reducer for facts
 *
 * This is an almost a pass through, most of the work is done via partition & sort.
 *
 * The input key is a long, where the top 32 bits is an externalized feature id that we can
 * use to lookup the namespace, and the bottom 32 bits is an ivory date representation that we
 * can use to determine the partition to write out to.
 *
 * The input value is the bytes representation of the fact ready to write out.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized Fact. The output
 * is partitioned by namespace and date (determined by the input key).
 */
class FactsReducer extends Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable] {
  var ctx: MrContext = null
  var out: MultipleOutputs[NullWritable, BytesWritable] = null
  var lookup: NamespaceLookup = new NamespaceLookup

  override def setup(context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.NamespaceLookup, lookup)
    out = new MultipleOutputs(context)
  }

  override def cleanup(context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit =
    out.close()

  override def reduce(key: LongWritable, iter: java.lang.Iterable[BytesWritable], context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    val path = ReducerLookups.factsetPartitionFor(lookup, key)
    val iterator = iter.iterator
    while (iterator.hasNext)
      out.write(FactsetJobKeys.Out, NullWritable.get, iterator.next, path)
  }
}





