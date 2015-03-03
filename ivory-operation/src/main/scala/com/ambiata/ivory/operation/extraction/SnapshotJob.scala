package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.{FeatureIdLookup, SnapshotWindowLookup, FlagLookup, NamespaceLookup}
import com.ambiata.ivory.operation.extraction.mode.ModeReducer
import com.ambiata.ivory.operation.extraction.snapshot._, SnapshotWritable._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.lookup._
import com.ambiata.ivory.storage.plan._
import com.ambiata.ivory.mr._
import com.ambiata.mundane.control._
import com.ambiata.poacher.mr._

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.{Counter => _, _}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat, MultipleOutputs, LazyOutputFormat}

/**
 * This is a hand-coded MR job to squeeze the most out of snapshot performance.
 */
object SnapshotJob {

  def run(repository: HdfsRepository, plan: SnapshotPlan, reducers: Int, output: Path): RIO[SnapshotStats] = {

    val job = Job.getInstance(repository.configuration)
    val ctx = MrContextIvory.newContext("ivory-snapshot", job)

    job.setJarByClass(classOf[SnapshotReducer])
    job.setJobName(ctx.id.value)

    // map
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    // partition & sort
    job.setPartitionerClass(classOf[SnapshotWritable.PartitionerEntityFeatureId])
    job.setGroupingComparatorClass(classOf[SnapshotWritable.GroupingEntityFeatureId])
    job.setSortComparatorClass(classOf[SnapshotWritable.Comparator])

    // reducer
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[SnapshotReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    // input
    IvoryInputs.configure(ctx, job, repository, plan.datasets, {
      case FactsetFormat.V1 => classOf[SnapshotV1FactsetMapper]
      case FactsetFormat.V2 => classOf[SnapshotV2FactsetMapper]
    }, {
      case SnapshotFormat.V1 => classOf[SnapshotV1IncrementalMapper]
      case SnapshotFormat.V2 => classOf[SnapshotV2IncrementalMapper]
    })

    // output
    LazyOutputFormat.setOutputFormatClass(job, classOf[SequenceFileOutputFormat[_, _]])
    MultipleOutputs.addNamedOutput(job, Keys.Out, classOf[SequenceFileOutputFormat[_, _]],  classOf[IntWritable], classOf[BytesWritable])
    FileOutputFormat.setOutputPath(job, ctx.output)

    // compression
    repository.codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    // cache / config initializtion
    job.getConfiguration.set(Keys.SnapshotDate, plan.date.int.toString) // FIX why int toString and not render/parse date?
    ctx.thriftCache.push(job, Keys.FactsetLookup, FactsetLookups.priorityTable(plan.datasets))
    ctx.thriftCache.push(job, Keys.FactsetVersionLookup, FactsetLookups.versionTable(plan.datasets))
    val (featureIdLookup, windowLookup) = windowTable(plan.commit.dictionary.value, plan.commit.dictionary.value.windows.byFeature(plan.date))
    ctx.thriftCache.push(job, Keys.FeatureIdLookup, featureIdLookup)
    ctx.thriftCache.push(job, Keys.WindowLookup, windowLookup)
    ctx.thriftCache.push(job, Keys.FeatureIsSetLookup, FeatureLookups.isSetTable(plan.commit.dictionary.value))
    ctx.thriftCache.push(job, ReducerLookups.Keys.NamespaceLookup, ReducerLookups.index(plan.commit.dictionary.value)._1)

    // run job
    if (!job.waitForCompletion(true))
      Crash.error(Crash.RIO, "ivory snapshot failed.")

    // commit files to factset
    for {
      _ <- Committer.commit(ctx, {
        case "snapshot" => output
      }, true).run(repository.configuration)
      now <- RIO.fromIO(DateTime.now)
    } yield {
      val featureIdToName = featureIdLookup.ids.asScala.mapValues(_.toString).map(_.swap)
      SnapshotStats(IvoryVersion.get, now, job.getCounters.getGroup(Keys.CounterFeatureGroup).iterator().asScala.flatMap {
        c => featureIdToName.get(c.getName).flatMap(FeatureId.parse(_).toOption).map(_ -> c.getValue)
      }.toMap)
    }
  }

  def windowTable(dictionary: Dictionary, ranges: Ranges[FeatureId]): (FeatureIdLookup, SnapshotWindowLookup) = {
    val featureIdLookup = FeatureLookups.featureIdTable(dictionary)
    val windowLookup = new SnapshotWindowLookup(new java.util.HashMap[Integer, Integer])
    ranges.values.foreach(range => {
      val id = featureIdLookup.getIds.get(range.id.toString)
      windowLookup.putToWindow(id, range.fromOrMax.int)
    })
    (featureIdLookup, windowLookup)
  }

  object Keys {
    val SnapshotDate = "ivory.snapdate"
    val CounterFeatureGroup = "snapshot-counter-features"
    val FeatureIdLookup = ThriftCache.Key("feature-id-lookup")
    val FactsetLookup = ThriftCache.Key("factset-lookup")
    val FactsetVersionLookup = ThriftCache.Key("factset-version-lookup")
    val WindowLookup = ThriftCache.Key("factset-window-lookup")
    val FeatureIsSetLookup = ThriftCache.Key("feature-is-set-lookup")
    val Out = "out" // MultipleOutputs named output
  }
}

object SnapshotMapper {
  type MapperContext[K <: Writable] = Mapper[K, BytesWritable, BytesWritable, BytesWritable]#Context
}

/**
 * Factset mapper for ivory-snapshot.
 *
 * The input is a standard SequenceFileInputFormat. The path is used to determine the
 * factset/namespace/year/month/day, and a factset priority is pulled out of a lookup
 * table in the distributes cache.
 *
 * The output key is a string of entity|namespace|attribute
 *
 * The output value is expected (can not be typed checked because its all bytes) to be
 * a thrift serialized NamespacedFact object.
 */
abstract class SnapshotFactsetMapper[K <: Writable] extends CombinableMapper[K, BytesWritable, BytesWritable, BytesWritable] with MrFactFormat[K, BytesWritable] {
  import SnapshotMapper._

  /** Thrift deserializer. */
  val serializer = ThriftSerialiser()

  /** Context object holding dist cache paths */
  var ctx: MrContext = null

  /** Snapshot date, see #setup. */
  var strDate: String = null
  var date: Date = Date.unsafeFromInt(0)

  var priority = Priority.Max

  val kout = Writables.bytesWritable(4096)

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  var emitter: Emitter[BytesWritable, BytesWritable] = null

  /** Class to count number of non skipped facts, created once per mapper */
  var okCounter: Counter = null

  /** Class to count number of skipped facts, created once per mapper */
  var skipCounter: Counter = null

  /** Class to count number of dropped facts that don't appear in dictionary anymore, created once per mapper */
  var dropCounter: Counter = null

  /** Empty Fact, created once per mapper and mutated for each record */
  val fact = createMutableFact

  /** Class to convert a key/value into a Fact based of the version, created once per mapper */
  var converter: MrFactConverter[K, BytesWritable] = null

  val featureIdLookup = new FeatureIdLookup

  /** The format the mapper is reading from, set once per mapper from the subclass */
  val format: FactsetFormat

  final override def setupSplit(context: MapperContext[K], split: InputSplit): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    date = Date.fromInt(strDate.toInt).getOrElse(Crash.error(Crash.DataIntegrity, s"Invalid snapshot date '${strDate}'"))
    val factsetInfo: FactsetInfo = FactsetInfo.fromMr(ctx.thriftCache, SnapshotJob.Keys.FactsetLookup, context.getConfiguration, split)
    priority = factsetInfo.priority
    okCounter = MrCounter("ivory", s"snapshot.v${format.toStringFormat}.ok", context)
    skipCounter = MrCounter("ivory", s"snapshot.v${format.toStringFormat}.skip", context)
    dropCounter = MrCounter("ivory", "drop", context)
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FeatureIdLookup, featureIdLookup)
    emitter = MrContextEmitter(context)
    converter = factConverter(MrContext.getSplitPath(split))
  }

  /**
   * Map over thrift factsets, dropping any facts in the future of `date`
   *
   * This will create two counters:
   * 1. snapshot.<version>.ok - number of facts read
   * 2. snapshot.<version>.skip - number of facts skipped because they were in the future
   */
  override def map(key: K, value: BytesWritable, context: MapperContext[K]): Unit = {
    converter.convert(fact, key, value)
    val featureId = FeatureIdIndexOption.lookup(fact, featureIdLookup)
    if (featureId.isEmpty)
      dropCounter.count(1)
    else if (fact.date > date)
      skipCounter.count(1)
    else
      SnapshotFactsetMapper.map(fact, priority, kout, vout, emitter, okCounter, serializer, featureId.get)
  }
}

class SnapshotV1FactsetMapper extends SnapshotFactsetMapper[NullWritable] with MrFactsetFactFormatV1
class SnapshotV2FactsetMapper extends SnapshotFactsetMapper[NullWritable] with MrFactsetFactFormatV2

object SnapshotFactsetMapper {

  def map(fact: MutableFact, priority: Priority, kout: BytesWritable, vout: BytesWritable,
          emitter: Emitter[BytesWritable, BytesWritable], okCounter: Counter, deserializer: ThriftSerialiser,
          featureId: FeatureIdIndex) {
    okCounter.count(1)
    KeyState.set(fact, priority, kout, featureId)
    val bytes = deserializer.toBytes(fact)
    vout.set(bytes, 0, bytes.length)
    emitter.emit(kout, vout)
  }
}

/**
 * Incremental snapshot mapper.
 */
abstract class SnapshotIncrementalMapper[K <: Writable] extends CombinableMapper[K, BytesWritable, BytesWritable, BytesWritable] with MrFactFormat[K, BytesWritable] {
  import SnapshotMapper._

  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Empty Fact, created once per mapper and mutated for each record */
  val fact = createMutableFact

  /** Output key, created once per mapper and mutated for each record */
  val kout = Writables.bytesWritable(4096)

  /** Output value, created once per mapper and mutated for each record */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  var emitter: Emitter[BytesWritable, BytesWritable] = null

  /** Class to count number of non skipped facts, created once per mapper */
  var okCounter: Counter = null

  /** Class to count number of dropped facts that don't appear in dictionary anymore, created once per mapper */
  var dropCounter: Counter = null

  val featureIdLookup = new FeatureIdLookup

  /** Class to convert a key/value into a Fact based of the version, created once per mapper */
  var converter: MrFactConverter[K, BytesWritable] = null

  final override def setupSplit(context: MapperContext[K], split: InputSplit): Unit = {
    super.setup(context)
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FeatureIdLookup, featureIdLookup)
    okCounter = MrCounter("ivory", "snapshot.incr.ok", context)
    dropCounter = MrCounter("ivory", "drop", context)
    emitter = MrContextEmitter(context)
    converter = factConverter(MrContext.getSplitPath(split))
  }

  override def map(key: K, value: BytesWritable, context: MapperContext[K]): Unit = {
    converter.convert(fact, key, value)
    val featureId = FeatureIdIndexOption.lookup(fact, featureIdLookup)
    if (featureId.isEmpty)
      dropCounter.count(1)
    else
      SnapshotIncrementalMapper.map(fact, Priority.Max, kout, vout, emitter, okCounter, serializer, featureId.get)
  }
}

class SnapshotV1IncrementalMapper extends SnapshotIncrementalMapper[NullWritable] with MrSnapshotFactFormatV1
class SnapshotV2IncrementalMapper extends SnapshotIncrementalMapper[IntWritable] with MrSnapshotFactFormatV2

object SnapshotIncrementalMapper {
  def map(fact: MutableFact, priority: Priority, kout: BytesWritable, vout: BytesWritable,
          emitter: Emitter[BytesWritable, BytesWritable], okCounter: Counter, serializer: ThriftSerialiser,
          featureId: FeatureIdIndex) {
    okCounter.count(1)
    KeyState.set(fact, priority, kout, featureId)
    val bytes = serializer.toBytes(fact)
    vout.set(bytes, 0, bytes.length)
    emitter.emit(kout, vout)
  }
}

/**
 * Reducer for ivory-snapshot.
 *
 * This reducer takes the latest fact with the same entity|namespace|attribute key
 *
 * The input values are serialized containers of factset priority and bytes of serialized NamespacedFact.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized NamespacedFact.
 */
class SnapshotReducer extends Reducer[BytesWritable, BytesWritable, IntWritable, BytesWritable] {
  import SnapshotReducer._

  /** Thrift deserialiser */
  val serialiser = ThriftSerialiser()

  /** Empty Fact, created once per reducer and mutated per record */
  val fact: MutableFact = createMutableFact

  /** Output key, created once per reducer and mutated per record */
  val kout = new IntWritable(0)

  /** Output value, created once per reducer and mutated per record */
  val vout = Writables.bytesWritable(4096)

  /** Optimised array lookup for features to the window, where we know that features are simply ordered from zero */
  var windowLookup: Array[Int] = null

  /** Optimised array lookup to flag "Set" features vs "State" features. */
  var modeReducer: Array[ModeReducer] = null
  var feaureIdStrings: Array[String] = null

  var featureCounter: LabelledCounter = null

  var namespaceLookup: NamespaceLookup = new NamespaceLookup

  var out: MultipleOutputs[IntWritable, BytesWritable] = null

  override def setup(context: ReducerContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)

    val windowLookupThrift = new SnapshotWindowLookup
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.WindowLookup, windowLookupThrift)
    windowLookup = windowLookupToArray(windowLookupThrift)

    val isSetLookupThrift = new FlagLookup
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FeatureIsSetLookup, isSetLookupThrift)
    modeReducer = ModeReducer.fromLookup(isSetLookupThrift)

    featureCounter = new MrLabelledCounter(SnapshotJob.Keys.CounterFeatureGroup, context)
    // Kinda sucky for debugging - we're using ints for counters because Hadoop limits the size of counter names to 64
    feaureIdStrings = FeatureLookups.sparseMapToArray(windowLookupThrift.getWindow.asScala.toList
      .map { case (fid, _) => fid.intValue() -> fid.toString }, "")

    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.NamespaceLookup, namespaceLookup)

    out = new MultipleOutputs(context)
  }

  override def cleanup(context: ReducerContext): Unit =
    out.close()

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: ReducerContext): Unit = {
    val feature: Int = SnapshotWritable.GroupingEntityFeatureId.getFeatureId(key)
    val windowStart: Date = Date.unsafeFromInt(windowLookup(feature))
    // emit to namespaced subdirs
    val emitter = MrOutputEmitter(SnapshotJob.Keys.Out, out, "snapshot/" + namespaceLookup.namespaces.get(feature) + "/part")
    val count: Int = SnapshotReducer.reduce(fact, iter.iterator, emitter, kout, vout, windowStart, modeReducer(feature), serialiser)
// FIX MAX COUNTERS    featureCounter.count(feaureIdStrings(feature), count)
    ()
  }
}

/** ***************** !!!!!! WARNING !!!!!! ******************
 *
 * There is some nasty mutation in here that can corrupt data
 * without knowing, so double/triple check with others when
 * changing.
 *
 ********************************************************** */
object SnapshotReducer {
  type ReducerContext = Reducer[BytesWritable, BytesWritable, IntWritable, BytesWritable]#Context

  def reduce(fact: MutableFact, iter: JIterator[BytesWritable], emitter: Emitter[IntWritable, BytesWritable],
             kout: IntWritable, vout: BytesWritable, windowStart: Date, mode: ModeReducer, serialiser: ThriftSerialiser): Int = {

    var count = 0
    var first = true
    var modeState = mode.seed
    while(iter.hasNext) {
      val next = iter.next
      ThriftByteMutator.from(next, fact, serialiser)
      // Respect the "highest" priority (ie. the first fact with any given datetime), unless this is a set
      // then we want to include every value (at least until we have keyed sets, see https://github.com/ambiata/ivory/issues/376).
      if (mode.accept(modeState, fact)) {
        // If the _current_ fact is in the window we still want to emit the _previous_ fact which may be
        // the last fact within the window, or another fact within the window
        // As such we can't do anything on the first fact
        if (!first && Window.isFactWithinWindow(windowStart, fact)) {
          emitter.emit(kout, vout)
          count += 1
        }
        first = false
        modeState = mode.step(modeState, fact)
        // Store the current fact, which may or may not be emitted depending on the next fact
        kout.set(fact.date.int)
        ThriftByteMutator.mutate(fact.toThrift, vout, serialiser)
      }
    }
    // _Always_ emit the last fact, which will be within the window, or the last fact
    emitter.emit(kout, vout)
    count += 1
    count
  }

  def windowLookupToArray(lookup: SnapshotWindowLookup): Array[Int] =
    FeatureLookups.sparseMapToArray(lookup.getWindow.asScala.map { case (i, v) => i.toInt -> v.toInt}.toList, Date.maxValue.int)
}
