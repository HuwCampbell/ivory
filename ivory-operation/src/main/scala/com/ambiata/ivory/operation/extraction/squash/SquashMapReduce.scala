package com.ambiata.ivory.operation.extraction.squash

import java.lang.{Iterable => JIterable}

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.NamespacedThriftFact
import com.ambiata.ivory.lookup.{ChordEntities, FeatureIdLookup, FeatureReductionLookup}
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.{ChordJob, Entities, SnapshotJob}
import com.ambiata.poacher.mr._
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

trait SquashReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {

  type ReducerContext = com.ambiata.ivory.operation.extraction.SnapshotReducer.ReducerContext

  val emitter = MrEmitter[BytesWritable, BytesWritable, NullWritable, BytesWritable]()
  val vout = Writables.bytesWritable(4096)

  val factEmitter = new FactByteMutator
  val lookup = new FeatureReductionLookup()
  val fact = createMutableFact
  val emitFact = createMutableFact
  var state: SquashReducerState = null
  var tracer: SquashProfiler = null

  def createState(context: ReducerContext): SquashReducerState

  override def setup(context: ReducerContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)

    ctx.thriftCache.pop(context.getConfiguration, SquashJob.Keys.ExpressionLookup, lookup)
    val traceMod = context.getConfiguration.getInt(SquashJob.Keys.ProfileMod, SquashConfig.default.profileSampleRate)
    def newCounter(group: String): String => Counter =
      name => MrCounter[BytesWritable, BytesWritable, NullWritable, BytesWritable](group, name, context)
    tracer = new SquashProfiler(traceMod, newCounter(SquashJob.Keys.CounterTotalGroup), newCounter(SquashJob.Keys.CounterSaveGroup),
      newCounter(SquashJob.Keys.ProfileTotalGroup), newCounter(SquashJob.Keys.ProfileSaveGroup))
    state = createState(context)
  }

  override def reduce(key: BytesWritable, iterable: JIterable[BytesWritable], context: ReducerContext): Unit = {
    emitter.context = context

    // Compiling an expression is (eventually) going to get more expensive, and so we only want to do it on demand
    // For this reason we sort by featureId and compile once here, and process all the entities
    val pool = ReducerPool.create(lookup.getReductions.get(SquashWritable.GroupingByFeatureId.getFeatureId(key)).asScala.toList, tracer.wrap)
    state.reduceAll(fact, emitFact, pool, factEmitter, iterable.iterator, emitter, vout)
  }
}

class SquashReducerSnapshot extends SquashReducer {

  override def createState(context: ReducerContext): SquashReducerState = {
    val strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    val date = Date.fromInt(strDate.toInt).getOrElse(Crash.error(Crash.DataIntegrity, s"Invalid snapshot date '$strDate'"))
    new SquashReducerStateSnapshot(date)
  }
}

class SquashReducerChord extends SquashReducer {

  override def createState(context: ReducerContext): SquashReducerState = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val entities = new ChordEntities
    ctx.thriftCache.pop(context.getConfiguration, ChordJob.Keys.ChordEntitiesLookup, entities)
    new SquashReducerStateChord(Entities.fromChordEntities(entities))
  }
}
