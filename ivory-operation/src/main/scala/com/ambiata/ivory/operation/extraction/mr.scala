package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.{FactsetLookup, FactsetVersionLookup}
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable, SnapshotWritable._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.mr._
import com.ambiata.mundane.io.FilePath

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}

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
    job.setPartitionerClass(classOf[SnapshotWritable.SPartitioner])
    job.setGroupingComparatorClass(classOf[SnapshotWritable.Grouping])
    job.setSortComparatorClass(classOf[SnapshotWritable.Comparator])

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
 * factset/namespace/year/month/day, and a factset priority is pulled out of a lookup
 * table in the distributes cache.
 *
 * The output key is a string of entity|namespace|attribute
 *
 * The output value is expected (can not be typed checked because its all bytes) to be
 * a thrift serialized NamespacedFact object.
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

  /** Thrift object provided from sub class, created once per mapper */
  val tfact = new ThriftFact

  /** Class to convert a Thrift fact into a Fact based of the version, created once per mapper */
  var converter: VersionedFactConverter = null

  override def setup(context: MapperContext): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    date = Date.fromInt(strDate.toInt).getOrElse(Crash.error(Crash.DataIntegrity, s"Invalid snapshot date '${strDate}'"))
    val (factsetVersion, vfc, p) = SnapshotFactsetMapper.setupVersionAndPriority(ctx.thriftCache,
      context.getConfiguration, context.getInputSplit)
    converter = vfc
    priority = p
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
    SnapshotFactsetMapper.map(tfact, date, converter, value, priority, kout, vout, emitter, okCounter, skipCounter, serializer)
  }
}

object SnapshotFactsetMapper {

  def map[A <: ThriftLike](tfact: ThriftFact, date: Date, converter: VersionedFactConverter, input: BytesWritable,
                           priority: Priority, kout: BytesWritable, vout: BytesWritable, emitter: Emitter[BytesWritable, BytesWritable],
                           okCounter: Counter, skipCounter: Counter, deserializer: ThriftSerialiser) {
    deserializer.fromBytesViewUnsafe(tfact, input.getBytes, 0, input.getLength)
    val f = converter.convert(tfact)
    if(f.date > date)
      skipCounter.count(1)
    else {
      okCounter.count(1)
      KeyState.set(f, priority, kout)
      val bytes = deserializer.toBytes(f.toNamespacedThrift)
      vout.set(bytes, 0, bytes.length)
      emitter.emit(kout, vout)
    }
  }

  def setupVersionAndPriority(thriftCache: ThriftCache, configuration: Configuration, inputSplit: InputSplit): (FactsetVersion, VersionedFactConverter, Priority) = {
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
    (factsetVersion, converter, Priority.unsafe(priority))
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

  /** Class to emit the key/value bytes, created once per mapper */
  val emitter: MrEmitter[NullWritable, BytesWritable, BytesWritable, BytesWritable] = MrEmitter()

  /** Class to count number of non skipped facts, created once per mapper */
  val okCounter: MrCounter[NullWritable, BytesWritable, BytesWritable, BytesWritable] =
    MrCounter("ivory", "snapshot.incr.ok")

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    emitter.context = context
    okCounter.context = context
    SnapshotIncrementalMapper.map(fact, value, Priority.Max, kout, vout, emitter, okCounter, serializer)
  }
}

object SnapshotIncrementalMapper {

  def map(fact: NamespacedThriftFact with NamespacedThriftFactDerived, bytes: BytesWritable, priority: Priority,
          kout: BytesWritable, vout: BytesWritable, emitter: Emitter[BytesWritable, BytesWritable], okCounter: Counter,
          serializer: ThriftSerialiser) {
    okCounter.count(1)
    fact.clear()
    serializer.fromBytesViewUnsafe(fact, bytes.getBytes, 0, bytes.getLength)
    KeyState.set(fact, priority, kout)
    // Pass through the bytes
    vout.set(bytes.getBytes, 0, bytes.getLength)
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
class SnapshotReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {
  import SnapshotReducer._

  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Empty Fact, created once per reducer and mutated per record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /** Output value, created once per reducer and mutated per record */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  val emitter: MrEmitter[BytesWritable, BytesWritable, NullWritable, BytesWritable] = MrEmitter()

  /** Class to count number of tombstone values, created once per mapper */
  val counter: MrCounter[BytesWritable, BytesWritable, NullWritable, BytesWritable] =
    MrCounter("ivory", "snapshot.reducer.tombstone")

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: ReducerContext): Unit = {
    emitter.context = context
    counter.context = context
    SnapshotReducer.reduce(fact, vout, iter.iterator, emitter, counter, serializer)
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

  def reduce(fact: NamespacedThriftFact with NamespacedThriftFactDerived, vout: BytesWritable,
             iter: JIterator[BytesWritable], emitter: Emitter[NullWritable, BytesWritable], counter: Counter,
             serializer: ThriftSerialiser) {
    var datetime = DateTime.unsafeFromLong(0)
    var tombstone = true
    // We are expecting a stream of facts grouped by entity/feature and sorted ascending by datetime and priority
    while (iter.hasNext) {
      val next = iter.next
      serializer.fromBytesViewUnsafe(fact, next.getBytes, 0, next.getLength)
      // Capture the bytes of the highest priority entity of a given datetime, but we only emit the last one
      if (datetime != fact.datetime) {
        vout.set(next.getBytes, 0, next.getLength)
        datetime = fact.datetime
        tombstone = fact.isTombstone
      }
    }
    if (!tombstone) emitter.emit(NullWritable.get(), vout)
    else counter.count(1)
  }
}
