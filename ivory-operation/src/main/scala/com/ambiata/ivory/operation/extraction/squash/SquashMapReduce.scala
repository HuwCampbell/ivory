package com.ambiata.ivory.operation.extraction.squash

import java.lang.{Iterable => JIterable}

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.{NamespacedThriftFact, ThriftSerialiser}
import com.ambiata.ivory.lookup.{FeatureReduction, FeatureIdLookup, FeatureReductionLookup}
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.reduction.Reduction
import com.ambiata.ivory.operation.extraction.SnapshotJob
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}

import scala.collection.JavaConverters._

class SquashMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  import com.ambiata.ivory.operation.extraction.SnapshotMapper._

  val serializer = ThriftSerialiser()
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived
  val kout = Writables.bytesWritable(4096)
  val vout = Writables.bytesWritable(4096)
  val lookup = new FeatureIdLookup

  val emitter: MrEmitter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = MrEmitter()

  override def setup(context: MapperContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FeatureIdLookup, lookup)
  }

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    serializer.fromBytesViewUnsafe(fact, value.getBytes, 0, value.getLength)
    emitter.context = context
    SquashWritable.KeyState.set(fact, kout, lookup.getIds.get(fact.featureId.toString))
    vout.set(value.getBytes, 0, value.getLength)
    emitter.emit(kout, vout)
  }
}

class SquashReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {
  import com.ambiata.ivory.operation.extraction.SnapshotReducer._

  val emitter = MrEmitter[BytesWritable, BytesWritable, NullWritable, BytesWritable]()
  val vout = Writables.bytesWritable(4096)
  val factEmitter = new FactByteMutator

  val lookup = new FeatureReductionLookup()
  val fact = createMutableFact
  val emitFact = createMutableFact
  var state: SquashReducerState = null

  override def setup(context: ReducerContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)

    val strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    val date = Date.fromInt(strDate.toInt).getOrElse(Crash.error(Crash.DataIntegrity, s"Invalid snapshot date '$strDate'"))
    state = new SquashReducerState(date)

    ctx.thriftCache.pop(context.getConfiguration, SquashJob.Keys.ExpressionLookup, lookup)
  }

  override def reduce(key: BytesWritable, iterable: JIterable[BytesWritable], context: ReducerContext): Unit = {
    emitter.context = context

    // Compiling an expression is (eventually) going to get more expensive, and so we only want to do it on demand
    // For this reason we sort by featureId and compile once here, and process all the entities
    val reducers = SquashReducer.compileAll(lookup.getReductions.get(SquashWritable.GroupingByFeatureId.getFeatureId(key)).asScala.toList)
    state.reduceAll(fact, emitFact, reducers, factEmitter, iterable.iterator, emitter, vout)
  }
}

object SquashReducer {

  def compileAll(reductions: List[FeatureReduction]): List[(FeatureReduction, Reduction)] =
    reductions.flatMap {
      fr => Reduction.compile(fr).map(fr ->)
    }
}
