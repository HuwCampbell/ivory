package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.{FactsetLookup, FactsetVersionLookup}
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.mr._
import com.ambiata.mundane.io.FilePath

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}
import java.nio.ByteBuffer

import scalaz.{Name => _, Reducer => _, _}, Scalaz._

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
 * This is a hand-coded MR job to squeeze the most out of snapshot performance.
 */
object SnapshotJob {
  def run(conf: Configuration, reducers: Int, date: Date, inputs: List[Prioritized[FactsetGlob]], output: Path, incremental: Option[Path], codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("ivory-snapshot", job)

    job.setJarByClass(classOf[SnapshotReducer])
    job.setJobName(ctx.id.value)

    // map
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    // partition & sort
    job.setGroupingComparatorClass(classOf[Text.Comparator])
    job.setSortComparatorClass(classOf[Text.Comparator])

    // reducer
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[SnapshotReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    // input
    val mappers = inputs.map(p => (classOf[SnapshotFactsetMapper], p.value))
    mappers.foreach({ case (clazz, factsetGlob) =>
      factsetGlob.paths.foreach(path => {
        println(s"Input path: ${path.path}")
        MultipleInputs.addInputPath(job, path.toHdfs, classOf[SequenceFileInputFormat[_, _]], clazz)
      })
    })

    incremental.foreach(p => {
      println(s"Incremental path: ${p}")
      MultipleInputs.addInputPath(job, p, classOf[SequenceFileInputFormat[_, _]], classOf[SnapshotIncrementalMapper])
    })

    // output
    val tmpout = new Path(ctx.output, "snap")
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, tmpout)

    // compression
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    // cache / config initializtion
    job.getConfiguration.set(Keys.SnapshotDate, date.int.toString)
    ctx.thriftCache.push(job, Keys.FactsetLookup, priorityTable(inputs))
    ctx.thriftCache.push(job, Keys.FactsetVersionLookup, versionTable(inputs.map(_.value)))

    // run job
    if (!job.waitForCompletion(true))
      Crash.error(Crash.ResultTIO, "ivory snapshot failed.")

    // commit files to factset
    Committer.commit(ctx, {
      case "snap" => output
    }, true).run(conf).run.unsafePerformIO
    ()
  }

  def priorityTable(globs: List[Prioritized[FactsetGlob]]): FactsetLookup = {
    val lookup = new FactsetLookup
    globs.foreach(p => lookup.putToPriorities(p.value.factset.render, p.priority.toShort))
    lookup
  }

  def versionTable(globs: List[FactsetGlob]): FactsetVersionLookup = {
    val lookup = new FactsetVersionLookup
    globs.foreach(g => lookup.putToVersions(g.factset.render, g.version.toByte))
    lookup
  }

  object Keys {
    val SnapshotDate = "ivory.snapdate"
    val FactsetLookup = ThriftCache.Key("factset-lookup")
    val FactsetVersionLookup = ThriftCache.Key("factset-version-lookup")
  }
}

object SnapshotMapper {
  type MapperContext = Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context

  /**
   * Mutate a BytesWritable with the snapshot mapper key (entity, namespace, feature)
   */
  case class KeyState(capacity: Int) {
    val keyBytes = new Array[Byte](capacity)

    def set(f: Fact, state: BytesWritable) {
      val b1 = f.entity.getBytes
      val b2 = f.namespaceUnsafe.name.getBytes
      val b3 = f.feature.getBytes
      System.arraycopy(b1, 0, keyBytes, 0, b1.length)
      System.arraycopy(b2, 0, keyBytes, b1.length, b2.length)
      System.arraycopy(b3, 0, keyBytes, b1.length + b2.length, b3.length)
      state.set(keyBytes, 0, b1.length + b2.length + b3.length)
    }
  }

  /**
   * Mutate a PriorityTag with the serialized bytes of a fact, along with the factset
   * priority
   */
  case class ValueState(priority: Priority) {
    val priorityTag = new PriorityTag
    val serializer = ThriftSerialiser()

    def set[A](thrift: A, state: BytesWritable)(implicit ev: A <:< ThriftLike) {
      priorityTag.clear()
      priorityTag.setPriority(priority.toShort)
      priorityTag.setBytes(ByteBuffer.wrap(serializer.toBytes(thrift)))
      val bytes = serializer.toBytes(priorityTag)
      state.set(bytes, 0, bytes.length)
    }
  }
}

/** Version specific thrift converter */
sealed trait VersionedFactConverter {
  def convert(tfact: ThriftFact): Fact
}
case class VersionOneFactConverter(partition: Partition) extends VersionedFactConverter {
  def convert(tfact: ThriftFact): Fact =
    PartitionFactThriftStorageV1.createFact(partition, tfact)
}
case class VersionTwoFactConverter(partition: Partition) extends VersionedFactConverter {
  def convert(tfact: ThriftFact): Fact =
    PartitionFactThriftStorageV2.createFact(partition, tfact)
}

/**
 * Factset mapper for ivory-snapshot.
 *
 * The input is a standard SequenceFileInputFormat. The path is used to determine the
 * factset/namespace/year/month/day, and a factset priority is pull out of a lookup
 * table in the distributes cache.
 *
 * The output key is a string of entity|namespace|attribute
 *
 * The output value is expected (can not be typed checked because its all bytes) to be
 * a thrift serialized PriorityTag object. This is a container that holds a
 * factset priority and thrift serialized NamespacedFact object.
 */
class SnapshotFactsetMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  import SnapshotMapper._

  /** Thrift deserializer. */
  val serializer = ThriftSerialiser()

  /** Context object holding dist cache paths */
  var ctx: MrContext = null

  /** Snapshot date, see #setup. */
  var strDate: String = null
  var date: Date = Date.unsafeFromInt(0)

  /** Key state management, create once per mapper */
  val kstate = KeyState(4096)

  /** Value state management, create once per mapper */
  var vstate: ValueState = null

  /** The output key, only create once per mapper. */
  val kout = Writables.bytesWritable(4096)

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  var emitter: MrEmitter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = null

  /** Class to count number of non skipped facts, created once per mapper */
  var okCounter: MrCounter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = null

  /** Class to count number of skipped facts, created once per mapper */
  var skipCounter: MrCounter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = null

  /** Thrift object provided from sub class, created once per mapper */
  val tfact = new ThriftFact

  /** Class to convert a Thrift fact into a Fact based of the version, created once per mapper */
  var converter: VersionedFactConverter = null

  override def setup(context: MapperContext): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    date = Date.fromInt(strDate.toInt).getOrElse(Crash.error(Crash.DataIntegrity, s"Invalid snapshot date '${strDate}'"))
    val (factsetVersion, vfc, vs) = SnapshotFactsetMapper.setupVersionAndPriority(ctx.thriftCache,
      context.getConfiguration, context.getInputSplit)
    converter = vfc
    vstate = vs
    emitter = MrEmitter(kout, vout)
    okCounter = MrCounter("ivory", s"snapshot.v${factsetVersion}.ok")
    skipCounter = MrCounter("ivory", s"snapshot.v${factsetVersion}.skip")
  }

  /**
   * Map over thrift factsets, dropping any facts in the future of `date`
   *
   * This will create two counters:
   * 1. snapshot.<version>.ok - number of facts read
   * 2. snapshot.<version>.skip - number of facts skipped because they were in the future
   */
  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    emitter.context = context
    okCounter.context = context
    skipCounter.context = context
    SnapshotFactsetMapper.map(tfact, date, converter, value.copyBytes, kstate, vstate, kout, vout, emitter, okCounter, skipCounter, serializer)
  }
}

object SnapshotFactsetMapper {
  import SnapshotMapper._

  def map[A <: ThriftLike](tfact: ThriftFact, date: Date, converter: VersionedFactConverter, bytes: Array[Byte],
                           kstate: KeyState, vstate: ValueState, kout: BytesWritable, vout: BytesWritable,
                           emitter: Emitter, okCounter: Counter, skipCounter: Counter, deserializer: ThriftSerialiser) {
    deserializer.fromBytesUnsafe(tfact, bytes)
    val f = converter.convert(tfact)
    if(f.date > date)
      skipCounter.count(1)
    else {
      okCounter.count(1)
      kstate.set(f, kout)
      vstate.set(f.toNamespacedThrift, vout)
      emitter.emit()
    }
  }

  def setupVersionAndPriority(thriftCache: ThriftCache, configuration: Configuration, inputSplit: InputSplit): (FactsetVersion, VersionedFactConverter, SnapshotMapper.ValueState) = {
    val versionLookup = new FactsetVersionLookup <| (fvl => thriftCache.pop(configuration, SnapshotJob.Keys.FactsetVersionLookup, fvl))
    val path = FilePath(MrContext.getSplitPath(inputSplit).toString)
    val (factsetId, partition) = Factset.parseFile(path) match {
      case Success(r) => r
      case Failure(e) => Crash.error(Crash.DataIntegrity, s"Can not parse factset path ${e}")
    }
    val rawVersion = versionLookup.versions.get(factsetId.render)
    val factsetVersion = FactsetVersion.fromByte(rawVersion).getOrElse(Crash.error(Crash.DataIntegrity, s"Can not parse factset version '${rawVersion}'"))

    val converter = factsetVersion match {
      case FactsetVersionOne => VersionOneFactConverter(partition)
      case FactsetVersionTwo => VersionTwoFactConverter(partition)
    }
    val priorityLookup = new FactsetLookup <| (fl => thriftCache.pop(configuration, SnapshotJob.Keys.FactsetLookup, fl))
    val priority = priorityLookup.priorities.get(factsetId.render)
    val vstate = SnapshotMapper.ValueState(Priority.unsafe(priority))
    (factsetVersion, converter, vstate)
  }
}

/**
 * Incremental snapshot mapper.
 */
class SnapshotIncrementalMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  import SnapshotMapper._

  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Output key, created once per mapper and mutated for each record */
  val kout = Writables.bytesWritable(4096)

  /** Output value, created once per mapper and mutated for each record */
  val vout = Writables.bytesWritable(4096)

  /** Empty Fact, created once per mapper and mutated for each record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /** State management for mapper keys, created once per mapper and mutated per record */
  val kstate = KeyState(4096)

  /** State management for mapper values, created once per mapper and mutated per record */
  val vstate = ValueState(Priority.Max)

  /** Class to emit the key/value bytes, created once per mapper */
  var emitter: MrEmitter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = null

  /** Class to count number of non skipped facts, created once per mapper */
  val okCounter: MrCounter[NullWritable, BytesWritable, BytesWritable, BytesWritable] =
    MrCounter("ivory", "snapshot.incr.ok")

  override def setup(context: MapperContext): Unit = {
    super.setup(context)
    emitter = MrEmitter(kout, vout)
  }

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    emitter.context = context
    okCounter.context = context
    SnapshotIncrementalMapper.map(fact, value.copyBytes, kstate, vstate, kout, vout, emitter, okCounter, serializer)
  }
}

object SnapshotIncrementalMapper {
  import SnapshotMapper._

  def map(fact: NamespacedThriftFact with NamespacedThriftFactDerived, bytes: Array[Byte], kstate: KeyState,
          vstate: ValueState, kout: BytesWritable, vout: BytesWritable, emitter: Emitter, okCounter: Counter, serializer: ThriftSerialiser) {
    okCounter.count(1)
    fact.clear()
    serializer.fromBytesUnsafe(fact, bytes)
    kstate.set(fact, kout)
    vstate.set(fact, vout)
    emitter.emit()
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
class SnapshotReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {
  import SnapshotReducer._

  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Empty Fact, created once per reducer and mutated per record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /** Empty PriorityTag, created once per reducer and mutated per record */
  val priorityTag = new PriorityTag

  /** Output key, only create once per reducer */
  val kout = NullWritable.get

  /** Output value, created once per reducer and mutated per record */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  var emitter: MrEmitter[BytesWritable, BytesWritable, NullWritable, BytesWritable] = null

  /** Class to count number of tombstone values, created once per mapper */
  val counter: MrCounter[BytesWritable, BytesWritable, NullWritable, BytesWritable] =
    MrCounter("ivory", "snapshot.reducer.tombstone")

  override def setup(context: ReducerContext): Unit = {
    emitter = MrEmitter(kout, vout)
  }

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: ReducerContext): Unit = {
    emitter.context = context
    counter.context = context
    SnapshotReducer.reduce(fact, priorityTag, kout, vout, iter.iterator, emitter, counter, serializer)
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
  type ReducerContext = Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable]#Context

  case class ReduceState(var latestContainer: PriorityTag, var latestDate: Long, var isTombstone: Boolean) {
    def accept(priorityTag: PriorityTag, nextDate: Long): Boolean =
      latestContainer == null || nextDate > latestDate || (nextDate == latestDate && priorityTag.getPriority < latestContainer.getPriority)

    def save(fact: Fact, priorityTag: PriorityTag, nextDate: Long): Unit = {
      latestContainer = priorityTag.deepCopy
      latestDate = nextDate
      isTombstone = fact.isTombstone
    }

    def write(vout: BytesWritable, emitter: Emitter, counter: Counter): Unit =
      if (!isTombstone) {
        vout.set(latestContainer.getBytes, 0, latestContainer.getBytes.length)
        emitter.emit()
      } else {
        counter.count(1)
      }
  }

  def reduce(fact: NamespacedThriftFact with NamespacedThriftFactDerived, priorityTag: PriorityTag, kout: NullWritable,
             vout: BytesWritable, iter: JIterator[BytesWritable], emitter: Emitter, counter: Counter, serializer: ThriftSerialiser) {
    val state = ReduceState(null, 0l, true)
    while(iter.hasNext) {
      val next = iter.next
      priorityTag.clear()
      fact.clear()
      serializer.fromBytesUnsafe(priorityTag, next.getBytes) // populate PriorityTag which holds priority and serialized fact
      serializer.fromBytesUnsafe(fact, priorityTag.getBytes) // populate fact
      val nextDate = fact.datetime.long
      if (state.accept(priorityTag, nextDate))
        state.save(fact, priorityTag, nextDate)
    }
    state.write(vout, emitter, counter)
  }
}
