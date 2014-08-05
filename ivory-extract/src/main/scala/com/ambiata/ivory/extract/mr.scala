package com.ambiata.ivory.extract

import com.ambiata.poacher.hdfs._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.FactsetLookup
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.parse._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.mr._

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator}
import java.nio.ByteBuffer

import scalaz.{Reducer => _, _}, Scalaz._

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TSerializer, TDeserializer}

/*
 * This is a hand-coded MR job to squeeze the most out of snapshot performance.
 */
object SnapshotJob {
  def run(conf: Configuration, reducers: Int, date: Date, inputs: List[FactsetGlob], output: Path, incremental: Option[Path], codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("ivory-snapshot", job)

    job.setJarByClass(classOf[SnapshotReducer])
    job.setJobName(ctx.id.value)

    /* map */
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    /* partiton & sort */
    job.setGroupingComparatorClass(classOf[Text.Comparator])
    job.setSortComparatorClass(classOf[Text.Comparator])

    /* reducer */
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[SnapshotReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    /* input */
    val mappers = inputs.map({
      case FactsetGlob(FactsetVersionOne, factsets) => (classOf[SnapshotFactsetVersionOneMapper], factsets)
      case FactsetGlob(FactsetVersionTwo, factsets) => (classOf[SnapshotFactsetVersionTwoMapper], factsets)
    })
    mappers.foreach({ case (clazz, factsets) =>
      factsets.foreach({ case (_, ps) =>
        ps.foreach(p => {
          println(s"Input path: ${p.path}")
          MultipleInputs.addInputPath(job, new Path(p.path), classOf[SequenceFileInputFormat[_, _]], clazz)
        })
      })
    })

    incremental.foreach(p => {
      println(s"Incremental path: ${p}")
      MultipleInputs.addInputPath(job, p, classOf[SequenceFileInputFormat[_, _]], classOf[SnapshotIncrementalMapper])
    })

    /* output */
    val tmpout = new Path(ctx.output, "snap")
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, tmpout)

    /* compression */
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    /* cache / config initializtion */
    job.getConfiguration.set(Keys.SnapshotDate, date.int.toString)
    ctx.thriftCache.push(job, Keys.FactsetLookup, priorityTable(inputs))

    /* run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory snapshot failed.")

    /* commit files to factset */
    Committer.commit(ctx, {
      case "snap" => output
    }, true).run(conf).run.unsafePerformIO
  }

  def priorityTable(globs: List[FactsetGlob]): FactsetLookup = {
    val lookup = new FactsetLookup
    globs.foreach(_.factsets.foreach({ case (pfs, _) =>
      lookup.putToPriorities(pfs.factsetId.render, pfs.priority.toShort)
    }))
    lookup
  }

  object Keys {
    val SnapshotDate = "ivory.snapdate"
    val FactsetLookup = ThriftCache.Key("factset-lookup")
  }
}

object SnapshotMapper {
  type MapperContext = Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context

  /*
   * Mutate a BytesWritable with the snapshot mapper key (entity, namespace, feature)
   */
  case class KeyState(capacity: Int) {
    val keyBytes = new Array[Byte](capacity)

    def set(f: Fact, state: BytesWritable) {
      val b1 = f.entity.getBytes
      val b2 = f.namespace.getBytes
      val b3 = f.feature.getBytes
      System.arraycopy(b1, 0, keyBytes, 0, b1.length)
      System.arraycopy(b2, 0, keyBytes, b1.length, b2.length)
      System.arraycopy(b3, 0, keyBytes, b1.length + b2.length, b3.length)
      state.set(keyBytes, 0, b1.length + b2.length + b3.length)
    }
  }

  /*
   * Mutate a PriorityTag with the serialized bytes of a fact, along with the factset
   * priority
   */
  case class ValueState(priority: Priority) {
    val priorityTag = new PriorityTag
    val serializer = new TSerializer(new TCompactProtocol.Factory)

    def set(f: Fact, state: BytesWritable) {
      priorityTag.clear()
      priorityTag.setPriority(priority.toShort)
      priorityTag.setBytes(ByteBuffer.wrap(serializer.serialize(f.toNamespacedThrift)))
      val bytes = serializer.serialize(priorityTag)
      state.set(bytes, 0, bytes.length)
    }
  }

  def map(fact: Fact, kstate: KeyState, vstate: ValueState, kout: BytesWritable, vout: BytesWritable, commit: () => Unit, deserializer: TDeserializer) {
    kstate.set(fact, kout)
    vstate.set(fact, vout)
    commit()
  }
}

/*
 * Base mapper for ivory-snapshot.
 *
 * The input is a standard SequenceFileInputFormat. The path is used to determin the
 * factset/namespace/year/month/day, and a factset priority is pull out of a lookup
 * table in the distributes cache.
 *
 * The output key is a string of entity|namespace|attribute
 *
 * The output value is expected (can not be typed checked because its all bytes) to be
 * a thrift serialized PriorityTag object. This is a container that holds a
 * factset priority and thrift serialized NamespacedFact object.
 */
abstract class SnapshotFactsetBaseMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  import SnapshotMapper._

  /* Context object holding dist cache paths */
  var ctx: MrContext = null

  /* Snapshot date, see #setup. */
  var strDate: String = null
  var date: Date = Date.unsafeFromInt(0)

  /* Lookup table for facset priority */
  val lookup = new FactsetLookup

  /* Key state management, create once per mapper */
  val kstate = KeyState(4096)

  /* Value state management, create once per mapper */
  var vstate: ValueState = null

  /* The output key, only create once per mapper. */
  val kout = Writables.bytesWritable(4096)

  /* The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /* Partition created from input split path, only created once per mapper */
  var partition: Partition = null

  /* Input split path, only created once per mapper */
  var stringPath: String = null

  override def setup(context: MapperContext): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    strDate = context.getConfiguration.get(SnapshotJob.Keys.SnapshotDate)
    date = Date.fromInt(strDate.toInt).getOrElse(sys.error(s"Invalid snapshot date '${strDate}'"))
    ctx.thriftCache.pop(context.getConfiguration, SnapshotJob.Keys.FactsetLookup, lookup)
    stringPath = MrContext.getSplitPath(context.getInputSplit).toString
    partition = Partition.parseWith(stringPath) match {
      case Success(p) => p
      case Failure(e) => sys.error(s"Can not parse partition ${e}")
    }
    val priority = lookup.priorities.get(partition.factset.render)
    vstate = ValueState(Priority.unsafe(priority))
  }
}

trait SnapshotFactsetThiftMapper[A <: ThriftLike] extends SnapshotFactsetBaseMapper {
  import SnapshotMapper._

  /* Thrift deserializer. */
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Thrift object provided from sub class, created once per mapper */
  val thrift: A

  /* Version string provided from sub class and to be used for counter names, created once per mapper */
  val version: String

  /* Function to parse a fact given the path to the file containing the fact and the thrift fact.
   * Defined in the sub class */
  val parseFact: (String, A) => ParseError \/ Fact

  /* This is set through the setup method and will call parseFact above with the string path */
  var parse: A => ParseError \/ Fact = null

  var counterNameOk: String = null
  var counterNameSkip: String = null
  val counterGroup = "ivory"

  val commit: MapperContext => () => Unit = (context: MapperContext) => () => {
    context.write(kout, vout)
  }

  override def setup(context: MapperContext): Unit = {
    super.setup(context)
    counterNameOk = s"snapshot.${version}.ok"
    counterNameSkip = s"snapshot.${version}.skip"
    parse = (a: A) => parseFact(stringPath, a)
  }

  /*
   * Map over thrift factsets, dropping any facts in the future of `date`
   *
   * This will create two counters:
   * 1. snapshot.$version.ok - number of facts read
   * 2. snapshot.$version.skip - number of facts skipped because they were in the future
   */
  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    context.getCounter(counterGroup, counterNameOk).increment(1)
    if(!SnapshotFactsetThriftMapper.map(thrift, date, parse, value.copyBytes,
                                        kstate, vstate, kout, vout, commit(context), deserializer))
      context.getCounter(counterGroup, counterNameSkip).increment(1)
  }
}

object SnapshotFactsetThriftMapper {
  import SnapshotMapper._

  def map[A <: ThriftLike](thrift: A, date: Date, parseFact: A => ParseError \/ Fact, bytes: Array[Byte],
                           kstate: KeyState, vstate: ValueState, kout: BytesWritable, vout: BytesWritable,
                           commit: () => Unit, deserializer: TDeserializer): Boolean = {
    deserializer.deserialize(thrift, bytes)
    parseFact(thrift) match {
      case \/-(f) =>
        if(f.date > date)
          false
        else {
          SnapshotMapper.map(f, kstate, vstate, kout, vout, commit, deserializer)
          true
        }
      case -\/(e) =>
        sys.error(s"Can not read fact - ${e}")
    }
  }
}

/*
 * FactsetVersionOne mapper
 */
class SnapshotFactsetVersionOneMapper extends SnapshotFactsetThiftMapper[ThriftFact] {
  val thrift = new ThriftFact
  val version = "v1"
  val parseFact: (String, ThriftFact) => ParseError \/ Fact =
    PartitionFactThriftStorageV1.parseFact _
}

/*
 * FactsetVersionTwo mapper
 */
class SnapshotFactsetVersionTwoMapper extends SnapshotFactsetThiftMapper[ThriftFact] {
  val thrift = new ThriftFact
  val version = "v2"
  val parseFact: (String, ThriftFact) => ParseError \/ Fact =
    PartitionFactThriftStorageV2.parseFact _
}

/*
 * Incremental snapshot mapper.
 */
class SnapshotIncrementalMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  import SnapshotMapper._

  /* Thrift deserializer */
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Output key, created once per mapper and mutated for each record */
  val kout = Writables.bytesWritable(4096)

  /* Output value, created once per mapper and mutated for each record */
  val vout = Writables.bytesWritable(4096)

  /* Empty Fact, created once per mapper and mutated for each record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /* State management for mapper keys, created once per mapper and mutated per record */
  val kstate = KeyState(4096)

  /* State management for mapper values, created once per mapper and mutated per record */
  val vstate = ValueState(Priority.Max)

  val counterNameOk = "snapshot.incr.ok"
  val counterGroup = "ivory"

  val commit: (MapperContext) => () => Unit = (context: MapperContext) => () => {
    context.write(kout, vout)
  }

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    context.getCounter(counterGroup, counterNameOk).increment(1)
    SnapshotIncrementalMapper.map(fact, value.copyBytes, kstate, vstate, kout, vout, commit(context), deserializer)
  }
}

object SnapshotIncrementalMapper {
  import SnapshotMapper._

  def map(fact: NamespacedThriftFact with NamespacedThriftFactDerived, bytes: Array[Byte], kstate: KeyState,
          vstate: ValueState, kout: BytesWritable, vout: BytesWritable, commit: () => Unit, deserializer: TDeserializer) {
    fact.clear()
    deserializer.deserialize(fact, bytes)
    SnapshotMapper.map(fact, kstate, vstate, kout, vout, commit, deserializer)
  }
}

/*
 * Reducer for ivory-snapshot.
 *
 * This reducer takes the latest fact with the same entity|namespace|attribute key
 *
 * The input values are serialized conainers of factset priority and bytes of serialized NamespacedFact.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized NamespacedFact.
 */
class SnapshotReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, BytesWritable] {
  import SnapshotReducer._

  /* Thrift deserializer */
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  /* Empty Fact, created once per reducer and mutated per record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

  /* Empty PriorityTag, created once per reducer and mutated per record */
  val priorityTag = new PriorityTag

  /* Output key, only create once per reducer */
  val kout = NullWritable.get

  /* Output value, created once per reducer and mutated per record */
  val vout = Writables.bytesWritable(4096)

  val commit: (ReducerContext) => () => Unit = (context: ReducerContext) => () => {
    context.write(kout, vout)
  }

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: ReducerContext): Unit = {
    SnapshotReducer.reduce(fact, priorityTag, kout, vout, iter.iterator, commit(context), deserializer)
  }
}

/* ***************** !!!!!! WARNING !!!!!! ******************
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

    def write(vout: BytesWritable, commit: () => Unit): Unit =
      if (!isTombstone) {
        vout.set(latestContainer.getBytes, 0, latestContainer.getBytes.length)
        commit()
      }
  }

  def reduce(fact: NamespacedThriftFact with NamespacedThriftFactDerived, priorityTag: PriorityTag, kout: NullWritable,
             vout: BytesWritable, iter: JIterator[BytesWritable], commit: () => Unit, deserializer: TDeserializer) {
    val state = ReduceState(null, 0l, true)
    while(iter.hasNext) {
      val next = iter.next
      priorityTag.clear()
      fact.clear()
      deserializer.deserialize(priorityTag, next.getBytes) // populate PriorityTag which holds priority and serialized fact
      deserializer.deserialize(fact, priorityTag.getBytes) // populate fact
      val nextDate = fact.datetime.long
      if (state.accept(priorityTag, nextDate))
        state.save(fact, priorityTag, nextDate)
    }
    state.write(vout, commit)
  }
}
