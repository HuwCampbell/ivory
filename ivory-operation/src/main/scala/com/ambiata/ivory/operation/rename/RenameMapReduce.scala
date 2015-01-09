package com.ambiata.ivory.operation.rename

import java.lang.{Iterable => JIterable}

import com.ambiata.ivory.core.Priority
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.operation.extraction._
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.task.FactsetJobKeys
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.mr.{Counter => _, _}
import com.ambiata.poacher.mr._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

class RenameMapper extends CombinableMapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {

  type MapperContext = Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context

  /* Value state management, create once per mapper */
  var priority = Priority(0)

  /* The output key, only create once per mapper. */
  val kout = Writables.bytesWritable(4096)

  /* The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /* FactsetId created from input split path, only created once per mapper */
  val tfact = new ThriftFact

  val serializer = new ThriftSerialiser

  val mapping = new FeatureIdMapping

  var converter: VersionedFactConverter = null

  var counter: Counter = null

  override def setupSplit(context: MapperContext, split: InputSplit): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val factsetInfo: FactsetInfo = FactsetInfo.fromMr(ctx.thriftCache, SnapshotJob.Keys.FactsetLookup,
      SnapshotJob.Keys.FactsetVersionLookup, context.getConfiguration, split)
    converter = factsetInfo.factConverter
    priority = factsetInfo.priority
    ctx.thriftCache.pop(context.getConfiguration, RenameJob.Keys.Mapping, mapping)
    counter = context.getCounter("ivory", RenameJob.Keys.MapCounter)
  }

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    serializer.fromBytesViewUnsafe(tfact, value.getBytes, 0, value.getLength)
    val f = converter.convert(tfact)
    val to = mapping.getMapping.get(f.featureId.toString)
    if (to != null) {
      /***************************************************************
       * This is the actual business logic, just in case you missed it
       ***************************************************************/
      f.toThrift.setAttribute(to.newName)

      RenameWritable.KeyState.set(f, priority, kout, to.getFeatureId)

      val bytes = serializer.toBytes(f.toThrift)
      vout.set(bytes, 0, bytes.length)

      context.write(kout, vout)
      counter.increment(1)
    }
  }
}

class RenameReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {

  type ReducerContext = Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context

  var out: MultipleOutputs[NullWritable, BytesWritable] = null

  val lookup = new NamespaceLookup

  var counter: Counter = null

  val tfact = new ThriftFact

  val serializer = new ThriftSerialiser

  override def setup(context: ReducerContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.NamespaceLookup, lookup)
    out = new MultipleOutputs(context)
    counter = context.getCounter("ivory", RenameJob.Keys.ReduceCounter)
  }

  override def cleanup(context: ReducerContext): Unit =
    out.close()

  override def reduce(key: BytesWritable, iterable: JIterable[BytesWritable], context: ReducerContext): Unit = {
    // This is the most expensive part of this reducer, we need to calculate it as _little_ as possible
    val path = ReducerLookups.factsetPartitionForInt(lookup,
      RenameWritable.GroupingByFeatureIdDate.getFeatureId(key),
      RenameWritable.GroupingByFeatureIdDate.getDate(key))
    var previousTime = -1
    var previousEntity: String = null
    val iter = iterable.iterator()
    while (iter.hasNext) {
      val next = iter.next
      // Make sure we don't clobber different entities with the same date + time
      serializer.fromBytesViewUnsafe(tfact, next.getBytes, 0, next.getLength)
      // Because we are sorting by priority (ascending) we can ignore anything after the first unique time value
      if (previousTime != tfact.getSeconds || previousEntity != tfact.getEntity) {
        out.write(FactsetJobKeys.Out, NullWritable.get, next, path)
        previousTime = tfact.getSeconds
        previousEntity = tfact.getEntity
        counter.increment(1)
      }
    }
  }
}
