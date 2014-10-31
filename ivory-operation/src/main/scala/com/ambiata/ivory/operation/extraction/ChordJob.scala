package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.operation.extraction.chord._
import com.ambiata.ivory.operation.extraction.reduction.DateOffsets
import com.ambiata.ivory.operation.extraction.snapshot.{SnapshotWindows, SnapshotWritable}
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.lookup._
import com.ambiata.ivory.mr._
import com.ambiata.mundane.control._
import com.ambiata.poacher.mr._

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.{Counter => _, _}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat

/**
 * This is a hand-coded MR job to squeeze the most out of chord performance.
 *
 * This uses the SnapshotWritable in the exact same way snapshot does
 */
object ChordJob {
  def run(repository: HdfsRepository, reducers: Int, inputs: List[Prioritized[FactsetGlob]], output: Path, entities: Entities,
          dictionary: Dictionary, incremental: Option[Path], codec: Option[CompressionCodec], windowing: Boolean): ResultTIO[Unit] = {

    val job = Job.getInstance(repository.configuration)
    val ctx = MrContext.newContext("ivory-chord", job)

    job.setJarByClass(classOf[ChordReducer])
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
    job.setReducerClass(classOf[ChordReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    // input
    val mappers = inputs.map(p => (classOf[ChordFactsetMapper], p.value))
    mappers.foreach({ case (clazz, factsetGlob) =>
      factsetGlob.keys.foreach(path => {
        println(s"Input path: ${path.name}")
        MultipleInputs.addInputPath(job, repository.toIvoryLocation(path).toHdfsPath, classOf[SequenceFileInputFormat[_, _]], clazz)
      })
    })

    incremental.foreach(p => {
      println(s"Incremental path: ${p}")
      MultipleInputs.addInputPath(job, p, classOf[SequenceFileInputFormat[_, _]], classOf[ChordIncrementalMapper])
    })

    // output
    val tmpout = new Path(ctx.output, "chord")
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, tmpout)

    // compression
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    // cache / config initializtion
    ctx.thriftCache.push(job, Keys.FactsetLookup, FactsetLookups.priorityTable(inputs))
    ctx.thriftCache.push(job, Keys.FactsetVersionLookup, FactsetLookups.versionTable(inputs.map(_.value)))
    ctx.thriftCache.push(job, Keys.FeatureIdLookup, featureIdLookup(dictionary))
    ctx.thriftCache.push(job, Keys.ChordEntitiesLookup, Entities.toChordEntities(entities))
    ctx.thriftCache.push(job, Keys.FeatureIsSetLookup, FeatureLookups.isSetTable(dictionary))
    job.getConfiguration.setBoolean(Keys.ChordWindowEnabled, windowing)
    if (windowing)
      ctx.thriftCache.push(job, Keys.ChordWindowsLookup, FeatureLookups.windowTable(dictionary))

    // run job
    if (!job.waitForCompletion(true))
      Crash.error(Crash.ResultTIO, "ivory chord failed.")

    // commit files to factset
    Committer.commit(ctx, {
      case "chord" => output
    }, true).run(repository.configuration)
  }

  def featureIdLookup(dict: Dictionary): FeatureIdLookup =
    new FeatureIdLookup(dict.byFeatureIndexReverse.map({ case (k, v) => (k.featureId.toString, Int.box(v)) }).asJava)

  def setupEntities(thriftCache: ThriftCache, configuration: Configuration): Entities = {
    val chordEntities = new ChordEntities
    thriftCache.pop(configuration, ChordJob.Keys.ChordEntitiesLookup, chordEntities)
    Entities.fromChordEntities(chordEntities)
  }

  object Keys {
    val ChordDate = "ivory.chorddate"
    val ChordWindowEnabled = "ivory.chord.window.enable"
    val FeatureIdLookup = ThriftCache.Key("feature-id-lookup")
    val FactsetLookup = ThriftCache.Key("factset-lookup")
    val FactsetVersionLookup = ThriftCache.Key("factset-version-lookup")
    val ChordEntitiesLookup = ThriftCache.Key("chord-entities-lookup")
    val FeatureIsSetLookup = ThriftCache.Key("feature-is-set-lookup")
    val ChordWindowsLookup = ThriftCache.Key("chord-window-lookup")
  }
}

object ChordMapper {
  type MapperContext = Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context

}

/**
 * Factset mapper for ivory-chord.
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
class ChordFactsetMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  import ChordMapper._

  /** Thrift deserializer. */
  val serializer = ThriftSerialiser()

  /** Context object holding dist cache paths */
  var ctx: MrContext = null

  var priority = Priority.Max

  val kout = Writables.bytesWritable(4096)

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  val emitter: MrEmitter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = MrEmitter()

  /** Class to count number of non skipped facts, created once per mapper */
  var okCounter: MrCounter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = null

  /** Class to count number of skipped facts, created once per mapper */
  var skipCounter: MrCounter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = null

  /** Class to count number of dropped facts that don't appear in dictionary anymore, created once per mapper */
  var dropCounter: LabelledCounter = null

  /** Thrift object provided from sub class, created once per mapper */
  val tfact = new ThriftFact

  /** Class to convert a Thrift fact into a Fact based of the version, created once per mapper */
  var converter: VersionedFactConverter = null

  val featureIdLookup = new FeatureIdLookup

  var entities: Entities = null

  override def setup(context: MapperContext): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    val factsetInfo: FactsetInfo = FactsetInfo.fromMr(ctx.thriftCache, ChordJob.Keys.FactsetLookup,
      ChordJob.Keys.FactsetVersionLookup, context.getConfiguration, context.getInputSplit)
    converter = factsetInfo.factConverter
    priority = factsetInfo.priority
    okCounter = MrCounter("ivory", s"chord.v${factsetInfo.version}.ok", context)
    skipCounter = MrCounter("ivory", s"chord.v${factsetInfo.version}.skip", context)
    dropCounter = MrLabelledCounter("ivory.drop", context)
    ctx.thriftCache.pop(context.getConfiguration, ChordJob.Keys.FeatureIdLookup, featureIdLookup)
    entities = ChordJob.setupEntities(ctx.thriftCache, context.getConfiguration)
  }

  /**
   * Map over thrift factsets, dropping any facts in the future of `date`
   *
   * This will create two counters:
   * 1. chord.<version>.ok - number of facts read
   * 2. chord.<version>.skip - number of facts skipped because they were in the future
   */
  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    emitter.context = context
    ChordFactsetMapper.map(tfact, converter, value, priority, kout, vout, emitter, okCounter, skipCounter, dropCounter, serializer,
      featureIdLookup, entities)
  }
}

object ChordFactsetMapper {

  // FIX VersionedFactConverter doesn't make sense, it is being called after deserialization into thrift????
  def map(tfact: ThriftFact, converter: VersionedFactConverter, input: BytesWritable, priority: Priority,
          kout: BytesWritable, vout: BytesWritable, emitter: Emitter[BytesWritable, BytesWritable],
          okCounter: Counter, skipCounter: Counter, dropCounter: LabelledCounter, deserializer: ThriftSerialiser,
          featureIdLookup: FeatureIdLookup, entities: Entities) {
    deserializer.fromBytesViewUnsafe(tfact, input.getBytes, 0, input.getLength)
    val f = converter.convert(tfact)
    val name = f.featureId.toString
    val featureId = featureIdLookup.getIds.get(name)
    if (featureId == null)
      dropCounter.count(name, 1)
    else if (!entities.keep(f))
      skipCounter.count(1)
    else {
      okCounter.count(1)
      SnapshotWritable.KeyState.set(f, priority, kout, featureId)
      val bytes = deserializer.toBytes(f.toNamespacedThrift)
      vout.set(bytes, 0, bytes.length)
      emitter.emit(kout, vout)
    }
  }
}

/**
 * Incremental chord mapper.
 */
class ChordIncrementalMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  import ChordMapper._

  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Output key, created once per mapper and mutated for each record */
  val kout = Writables.bytesWritable(4096)

  /** Output value, created once per mapper and mutated for each record */
  val vout = Writables.bytesWritable(4096)

  /** Empty Fact, created once per mapper and mutated for each record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /** Class to emit the key/value bytes, created once per mapper */
  val emitter: MrEmitter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = MrEmitter()

  /** Class to count number of non skipped facts, created once per mapper */
  var okCounter: Counter = null

  /** Class to count number of skipped facts, created once per mapper */
  var skipCounter: Counter =  null

  /** Class to count number of dropped facts that don't appear in dictionary anymore, created once per mapper */
  var dropCounter: LabelledCounter = null

  val featureIdLookup = new FeatureIdLookup

  var entities: Entities = null

  override def setup(context: MapperContext): Unit = {
    super.setup(context)
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, ChordJob.Keys.FeatureIdLookup, featureIdLookup)
    entities = ChordJob.setupEntities(ctx.thriftCache, context.getConfiguration)
    okCounter = MrCounter("ivory", "chord.incr.ok", context)
    skipCounter = MrCounter("ivory", "chord.incr.skip", context)
    dropCounter = MrLabelledCounter("ivory.drop", context)
  }

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    emitter.context = context
    ChordIncrementalMapper.map(fact, value, Priority.Max, kout, vout, emitter, okCounter, skipCounter, dropCounter, serializer, featureIdLookup, entities)
  }
}

object ChordIncrementalMapper {

  def map(fact: NamespacedThriftFact with NamespacedThriftFactDerived, bytes: BytesWritable, priority: Priority,
          kout: BytesWritable, vout: BytesWritable, emitter: Emitter[BytesWritable, BytesWritable], okCounter: Counter,
          skipCounter: Counter, dropCounter: LabelledCounter, serializer: ThriftSerialiser, featureIdLookup: FeatureIdLookup, entities: Entities) {
    serializer.fromBytesViewUnsafe(fact, bytes.getBytes, 0, bytes.getLength)
    val name = fact.featureId.toString
    val featureId = featureIdLookup.getIds.get(name)
    if (featureId == null)
      dropCounter.count(name, 1)
    else if(!entities.keep(fact))
      skipCounter.count(1)
    else {
      okCounter.count(1)
      SnapshotWritable.KeyState.set(fact, priority, kout, featureIdLookup.getIds.get(fact.featureId.toString))
      // Pass through the bytes
      vout.set(bytes.getBytes, 0, bytes.getLength)
      emitter.emit(kout, vout)
    }
  }
}

/**
 * Reducer for ivory-chord.
 *
 * This reducer takes the latest fact with the same entity|namespace|attribute key
 *
 * The input values are serialized containers of factset priority and bytes of serialized NamespacedFact.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized NamespacedFact.
 */
class ChordReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {
  import ChordReducer._

  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Empty Fact, created once per reducer and mutated per record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /** Output value, created once per reducer and mutated per record */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  val emitter: MrEmitter[BytesWritable, BytesWritable, NullWritable, BytesWritable] = MrEmitter()
  val chordNormalEmitter = new ChordNormalEmitter[BytesWritable](emitter)

  val mutator = new FactByteMutator

  var entities: Entities = null
  var featureWindows: Array[Option[Date => Date]] = null
  /** Shared array which can be re-used which is allocated size of the largest number of chords for a single entity */
  var windows: Array[Int] = null

  val buffer = new StringBuilder(4096)

  /** Optimised array lookup to flag "Set" features vs "State" features. */
  var isSetLookup: Array[Boolean] = null

  override def setup(context: ReducerContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    entities = ChordJob.setupEntities(ctx.thriftCache, context.getConfiguration)

    val isSetLookupThrift = new FlagLookup
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FeatureIsSetLookup, isSetLookupThrift)
    isSetLookup = FeatureLookups.isSetLookupToArray(isSetLookupThrift)

    if (context.getConfiguration.getBoolean(ChordJob.Keys.ChordWindowEnabled, false)) {
      featureWindows = ChordReducer.setupWindows(ctx.thriftCache, context.getConfiguration).map(_.map(Window.startingDate))
      windows = new Array(entities.maxChordSize)
    }
  }

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: ReducerContext): Unit = {
    emitter.context = context
    val entity = SnapshotWritable.GroupingEntityFeatureId.getEntity(key)
    val feature = SnapshotWritable.GroupingEntityFeatureId.getFeatureId(key)

    val chords = entities.entities.get(entity)
    val chordEmitter =
      if (featureWindows != null) {
        val featureId = SnapshotWritable.GroupingEntityFeatureId.getFeatureId(key)
        val dateLookup = featureWindows(featureId)
        // Using isDefined/get to avoid function allocation :(
        if (dateLookup.isDefined)
          ChordWindows.updateWindowsForChords(chords, dateLookup.get, windows)
        // The final code will/should not create this every time, but this is currently used in testing
        new ChordWindowEmitter(emitter, if (dateLookup.isDefined) windows else null)
      } else chordNormalEmitter

    ChordReducer.reduce(fact, iter.iterator, mutator, chordEmitter, vout, entities.entities.get(entity), buffer, isSetLookup(feature))
  }
}

/** ***************** !!!!!! WARNING !!!!!! ******************
 *
 * There is some nasty mutation in here that can corrupt data
 * without knowing, so double/triple check with others when
 * changing.
 *
 ********************************************************** */
object ChordReducer {
  type ReducerContext = Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context

  def setupWindows(thriftCache: ThriftCache, configuration: Configuration): Array[Option[Window]] = {
    val windows = new SnapshotWindowLookup
    thriftCache.pop(configuration, ChordJob.Keys.ChordWindowsLookup, windows)
    windowLookupToArray(windows)
  }

  def windowLookupToArray(windows: SnapshotWindowLookup): Array[Option[Window]] =
    FeatureLookups.sparseMapToArray(windows.window.asScala.map {
      case (fid, w) => fid.toInt ->  WindowLookup.fromInt(w)
    }.toList, None)

  val sentinelDateTime = DateTime.unsafeFromLong(-1)

  trait ChordEmitter[A] {
    val kout = NullWritable.get()

    def emit(fact: MutableFact, mutator: PipeFactMutator[A, A], out: A, dates: Array[Int], buffer: StringBuilder,
             previousDatetime: DateTime, date: Date, offset: Int): Int
  }

  class ChordNormalEmitter[A](emitter: Emitter[NullWritable, A]) extends ChordEmitter[A] {

    val delim = ':'

    def newEntityId(entity: String, date: Date, buffer: StringBuilder): String = {
      buffer.setLength(0)
      buffer.append(entity)
      buffer.append(delim)
      buffer.append(date.hyphenated)
      buffer.toString()
    }

    def emit(fact: MutableFact, mutator: PipeFactMutator[A, A], out: A, dates: Array[Int], buffer: StringBuilder,
             previousDatetime: DateTime, date: Date, offset: Int): Int = {
      var i = offset
      // Load the fact once from the cache
      mutator.from(out, fact)
      // Keep the original entity here because we're about to mutate the fact version
      val entity = fact.entity
      while (i >= 0 && date.underlying > dates(i)) {
        fact.fact.setEntity(newEntityId(entity, Date.unsafeFromInt(dates(i)), buffer))
        mutator.mutate(fact, out)
        emitter.emit(kout, out)
        i = i - 1
      }
      i
    }
  }

  /**
   * Currently "dead code", but will become the only implementation once squash has been extended to chord.
   * Unfortunately chord + window is impossible without squash (unlike snapshot) because of the entity rewriting.
   *
   * [[windowStarts]] will be the same length as `dates`, or `null` if no window is set for the current feature.
   */
  class ChordWindowEmitter[A](emitter: Emitter[NullWritable, A], windowStarts: Array[Int]) extends ChordEmitter[A] {

    def emit(fact: MutableFact, mutator: PipeFactMutator[A, A], out: A, dates: Array[Int], buffer: StringBuilder,
             previousDatetime: DateTime, date: Date, offset: Int): Int = {
      var i = offset
      // For window features _always_ emit the last fact before the window (for state-based features)
      // Keep in mind that this will _also_ handily emit the previous fact when it _is_ in the window
      var canEmit = windowStarts != null && windowStarts(i) <= date.underlying
      while (i >= 0 && date.underlying > dates(i)) {
        // For both types of features we _always_ want to emit the last fact for a given chord (it may be the only one)
        canEmit = true
        i = i - 1
      }
      if (canEmit) {
        // Because we're not messing with the entity ids any more we only need to emit the fact once
        // We can also emit directly what's currently stored in 'out' - no need to hydrate back into 'fact'
        emitter.emit(kout, out)
      }
      i
    }
  }

  def reduce[A](fact: MutableFact, iter: JIterator[A], mutator: PipeFactMutator[A, A],
                emitter: ChordEmitter[A], out: A, dates: Array[Int], buffer: StringBuilder, isSet: Boolean): Unit = {

    /**
     * Entity ids need to be appended with the date in the chord file as its possible to have the same entity
     * id with multiple dates in the chord file.
     */
    def emitEntity(previousDatetime: DateTime, date: Date, offset: Int): Int = {
      // If the first chord has no matches there won't be anything to emit
      // It also covers the (otherwise impossible) case that the iterator is empty
      if (previousDatetime != sentinelDateTime) {
        emitter.emit(fact, mutator, out, dates, buffer, previousDatetime, date, offset)
      } else {
        var i = offset
        // Ignore any old chords that don't have a matching fact
        while (i >= 0 && date.underlying > dates(i)) {
          i = i - 1
        }
        i
      }
    }
    // dates are ordered latest to earliest, but we want it the other way around
    var i = dates.length - 1
    var previousDatetime = sentinelDateTime
    while (iter.hasNext) {
      val next = iter.next
      mutator.from(next, fact)
      val datetime = fact.datetime
      // facts are in priority order already, so this simply takes the top priority when there is a date/time clash
      if (datetime != previousDatetime || isSet) {
        i = emitEntity(previousDatetime, datetime.date, i)
        previousDatetime = datetime
        // Store this fact to be emitted if we can't find a better match
        mutator.pipe(next, out)
      }
    }
    // Flush the remaining chords
    emitEntity(previousDatetime, Date.maxValue, i)
    ()
  }
}
