package com.ambiata.ivory.operation.extraction.squash

import java.lang.{Iterable => JIterable}

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.NamespacedThriftFact
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.reduction.Reduction
import com.ambiata.ivory.operation.extraction.{ChordJob, SnapshotJob}
import com.ambiata.ivory.storage.lookup.FeatureLookups
import com.ambiata.ivory.storage.entities._
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text, Writable}
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
    val featureIdString = fact.featureId.toString
    val featureId = lookup.getIds.get(featureIdString)
    if (featureId != null) {
      write(value, featureId.toInt, featureIdString)
    }
  }

  def write(value: BytesWritable, featureId: Int, featureIdString: String): Unit = {
    SquashWritable.KeyState.set(fact, kout, featureId)
    vout.set(value.getBytes, 0, value.getLength)
    emitter.emit(kout, vout)
  }
}

class SquashMapperFilter extends SquashMapper {
  import com.ambiata.ivory.operation.extraction.SnapshotMapper._

  var entities: Set[String] = null
  var features: Set[String] = null

  override def setup(context: MapperContext): Unit = {
    super.setup(context)
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val filter = new EntityFilterLookup
    ctx.thriftCache.pop(context.getConfiguration, SquashDumpJob.Keys.Filter, filter)
    entities = filter.getEntities.asScala.toSet
    features = filter.features.asScala.toSet
  }

  override def write(value: BytesWritable, featureId: Int, featureIdString: String): Unit = {
    if ((features.isEmpty || features.contains(featureIdString)) && entities.contains(fact.entity)) {
      super.write(value, featureId, featureIdString)
    }
  }
}

trait SquashReducer[A <: Writable] extends Reducer[BytesWritable, BytesWritable, NullWritable, A] {

  type ReducerContext = Reducer[BytesWritable, BytesWritable, NullWritable, A]#Context

  val emitter = MrEmitter[BytesWritable, BytesWritable, NullWritable, A]()
  val vout: A

  val factEmitter = new FactByteMutator
  val lookup = new FeatureReductionLookup()
  var isSetLookup: Array[Boolean] = null
  val fact = createMutableFact
  val emitFact = createMutableFact
  var state: SquashReducerState[A] = null
  var tracer: SquashProfiler = null

  def createState(context: ReducerContext): SquashReducerState[A]

  override def setup(context: ReducerContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)

    ctx.thriftCache.pop(context.getConfiguration, SquashJob.Keys.ExpressionLookup, lookup)
    val traceMod = context.getConfiguration.getInt(SquashJob.Keys.ProfileMod, SquashConfig.default.profileSampleRate)
    def newCounter(group: String): String => Counter =
      name => MrCounter[BytesWritable, BytesWritable, NullWritable, A](group, name, context)
    tracer = new SquashProfiler(traceMod, newCounter(SquashJob.Keys.CounterTotalGroup), newCounter(SquashJob.Keys.CounterSaveGroup),
      newCounter(SquashJob.Keys.ProfileTotalGroup), newCounter(SquashJob.Keys.ProfileSaveGroup))
    state = createState(context)

    val isSetLookupThrift = new FlagLookup
    ctx.thriftCache.pop(context.getConfiguration, SquashJob.Keys.FeatureIsSetLookup, isSetLookupThrift)
    isSetLookup = FeatureLookups.isSetLookupToArray(isSetLookupThrift)
  }

  override def reduce(key: BytesWritable, iterable: JIterable[BytesWritable], context: ReducerContext): Unit = {
    emitter.context = context

    val featureId = SquashWritable.GroupingByFeatureId.getFeatureId(key)
    val isSet = isSetLookup(featureId)
    // Compiling an expression is (eventually) going to get more expensive, and so we only want to do it on demand
    // For this reason we sort by featureId and compile once here, and process all the entities
    val pool = ReducerPool.create(lookup.getReductions.get(featureId).asScala.toList, isSet, trace)
    state.reduceAll(fact, emitFact, pool, factEmitter, iterable.iterator, emitter, vout)
  }

  def trace(fr: FeatureReduction, r: Reduction): Reduction =
    r /* FIX MAX COUNTERS tracer.wrap */
}

class SquashReducerSnapshot extends SquashReducer[BytesWritable] {

  val vout = Writables.bytesWritable(4096)

  override def createState(context: ReducerContext): SquashReducerState[BytesWritable] = {
    val strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    val date = Date.fromInt(strDate.toInt).getOrElse(Crash.error(Crash.DataIntegrity, s"Invalid snapshot date '$strDate'"))
    new SquashReducerStateSnapshot(date)
  }
}

class SquashReducerChord extends SquashReducer[BytesWritable] {

  val vout = Writables.bytesWritable(4096)

  override def createState(context: ReducerContext): SquashReducerState[BytesWritable] = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    val entities = new ChordEntities
    ctx.thriftCache.pop(context.getConfiguration, ChordJob.Keys.ChordEntitiesLookup, entities)
    new SquashReducerStateChord(Entities.fromChordEntities(entities))
  }
}

class SquashReducerDump extends SquashReducer[Text] {

  val vout = new Text

  override def createState(context: ReducerContext): SquashReducerState[Text] = {
    val strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    val date = Date.fromInt(strDate.toInt).getOrElse(Crash.error(Crash.DataIntegrity, s"Invalid snapshot date '$strDate'"))
    new SquashReducerStateDump(date)
  }

  override def trace(fr: FeatureReduction, r: Reduction): Reduction =
    SquashDump.wrap('|', "NA", fr, r, {
      line =>
        vout.set(line)
        emitter.emit(SquashReducerState.kout, vout)
    })
}
