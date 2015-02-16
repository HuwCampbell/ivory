package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.{FeatureIdLookup, NamespaceLookup, ThriftNumericalStats, ThriftCategoricalStats, ThriftHistogramEntry}
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.IvoryInputs
import com.ambiata.ivory.storage.lookup._
import com.ambiata.ivory.storage.fact._

import com.ambiata.mundane.control._
import com.ambiata.poacher.mr._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.{Counter => _, _}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner

import java.lang.{Iterable => JIterable}
import java.util.{Iterator => JIterator, HashMap}

import scala.collection.JavaConverters._

import scalaz.{Reducer => _, _}, Scalaz._

object FactStatsJob {
  def run(repository: HdfsRepository, dictionary: Dictionary, datasets: Datasets, target: HdfsIvoryLocation): RIO[Unit] = for {
    job   <- Job.getInstance(repository.configuration).pure[RIO]
    ctx    = MrContextIvory.newContext("ivory-fact-stats", job)
    stats <- RIO.safe {

      job.setJarByClass(classOf[FactStatsReducer])
      job.setJobName(ctx.id.value)

      /* map */
      job.setMapOutputKeyClass(classOf[BytesWritable])
      job.setMapOutputValueClass(classOf[BytesWritable])

      /* partition & sort */
      job.setPartitionerClass(classOf[HashPartitioner[_, _]])
      job.setGroupingComparatorClass(classOf[BytesWritable.Comparator])
      job.setSortComparatorClass(classOf[BytesWritable.Comparator])

      /* combiner */
      job.setCombinerClass(classOf[FactStatsCombiner])

      /* reducer */
      job.setNumReduceTasks(1)
      job.setReducerClass(classOf[FactStatsReducer])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[Text])

      /* input */
      IvoryInputs.configure(ctx, job, repository, datasets, {
        case FactsetFormat.V1 => classOf[FactV1StatsMapper]
        case FactsetFormat.V2 => classOf[FactV2StatsMapper]
      }, _ => Crash.error(Crash.Invariant, "FactStats should not be run on a snapshot!"))

      /* output */
      LazyOutputFormat.setOutputFormatClass(job, classOf[TextOutputFormat[_, _]])
      MultipleOutputs.addNamedOutput(job, Keys.Out, classOf[TextOutputFormat[_, _]],  classOf[NullWritable], classOf[Text])
      FileOutputFormat.setOutputPath(job, ctx.output)

      /* compression */
      repository.codec.foreach(cc => {
        Compress.intermediate(job, cc)
        Compress.output(job, cc)
      })

      /* cache / config initializtion */
      val featureIdLookup = FeatureLookups.featureIdTable(dictionary)
      ctx.thriftCache.push(job, Keys.FeatureIdLookup, featureIdLookup)

      /* run job */
      if (!job.waitForCompletion(true))
        Crash.error(Crash.RIO, "ivory fact stats failed.")
    }
  _ <- Committer.commit(ctx, {
      case "stats" => target.toHdfsPath
    }, true).run(repository.configuration)
  } yield stats

  object Keys {
    val Out = "out"
    val FeatureIdLookup = ThriftCache.Key("feature-id-lookup")
  }
}

abstract class FactStatsMapper[K <: Writable] extends CombinableMapper[K, BytesWritable, BytesWritable, BytesWritable] with MrFactFormat[K, BytesWritable] {

  type MapperContext = Mapper[K, BytesWritable, BytesWritable, BytesWritable]#Context

  /** Thrift serialiser. */
  val serialiser = ThriftSerialiser()

  /** Context object holding dist cache paths */
  var ctx: MrContext = null

  val kout = Writables.bytesWritable(4096)

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /** Class to emit the key/value bytes, created once per mapper */
  var emitter: Emitter[BytesWritable, BytesWritable] = null

  /** Empty Fact, created once per mapper and mutated for each record */
  val fact = createMutableFact

  val numericalStats = new ThriftNumericalStats()

  val categoricalStats = new ThriftCategoricalStats()

  /** Class to convert a key/value into a Fact based of the version, created once per mapper */
  var converter: MrFactConverter[K, BytesWritable] = null

  val featureIdLookup = new FeatureIdLookup

  final override def setupSplit(context: MapperContext, split: InputSplit): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    emitter = MrContextEmitter(context)
    ctx.thriftCache.pop(context.getConfiguration, FactStatsJob.Keys.FeatureIdLookup, featureIdLookup)
    converter = factConverter(MrContext.getSplitPath(split))
  }

  override def map(key: K, value: BytesWritable, context: MapperContext): Unit = {
    converter.convert(fact, key, value)
    FactStatsMapper.emitStats(fact, numericalStats, categoricalStats, emitter, kout, vout, featureIdLookup, serialiser)
  }
}

object FactStatsMapper {
  import spire.math._
  import spire.implicits._

  def emitStats(fact: Fact, numericalStats: ThriftNumericalStats, categoricalStats: ThriftCategoricalStats, emitter: Emitter[BytesWritable, BytesWritable],
                kout: BytesWritable, vout: BytesWritable, featureIdLookup: FeatureIdLookup, serialiser: ThriftSerialiser): Unit = {

    val name = fact.featureId.toString
    val featureId = featureIdLookup.getIds.get(name)

    def emitNumerical[A : Numeric](d: A): Unit = {
      FactStatsWritable.KeyState.set(featureId, fact.date, FactStatsWritable.Numerical, kout)
      numericalStats.setCount(d.toLong)
      numericalStats.setSum(d.toDouble)
      numericalStats.setSqsum(d.toDouble * d.toDouble)
      ThriftByteMutator.mutate(numericalStats, vout, serialiser)
      emitter.emit(kout, vout)
    }

    def emitCategorical(value: String) = {
      FactStatsWritable.KeyState.set(featureId, fact.date, FactStatsWritable.Categorical, kout)
      categoricalStats.setCategories(1)
      categoricalStats.setHistogram(List(new ThriftHistogramEntry(value, 1)).asJava)
      ThriftByteMutator.mutate(categoricalStats, vout, serialiser)
      emitter.emit(kout, vout)
    }

    fact.value match {
      case IntValue(i)      => emitNumerical(i); emitCategorical(i.toString)
      case LongValue(l)     => emitNumerical(l); emitCategorical(l.toString)
      case DoubleValue(d)   => emitNumerical(d); emitCategorical(d.toString)
      case TombstoneValue   => emitCategorical("☠")
      case StringValue(s)   => emitCategorical(s)
      case BooleanValue(b)  => emitCategorical(b.toString)
      case DateValue(r)     => emitCategorical(r.hyphenated)
      case ListValue(v)     => emitCategorical("List entries")
      case StructValue(m)   => emitCategorical("Struct entries")
    }
  }
}

class FactV1StatsMapper extends FactStatsMapper[NullWritable] with MrFactsetFactFormatV1
class FactV2StatsMapper extends FactStatsMapper[NullWritable] with MrFactsetFactFormatV2

/**
 * Combiner for fact stats.
 *
 * This combiner expects the following input:
 *   - The key to be feature id, date, and type (either Numerical or Categorical)
 *   - The value to be either:
 *       Numerical - ThriftNumericalStats
 *       Categorical - ThriftCategoricalStats
 *
 * The output will be the same format as the input so it can be fed into the combiner possibly multiple times.
 */
class FactStatsCombiner extends Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] {

  type CombinerContext = Reducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable]#Context

  /** Thrift serialiser. */
  val serialiser = ThriftSerialiser()

  /** Output value, created once per combiner and mutated per record */
  val vout = Writables.bytesWritable(4096)

  val numericalStats = new ThriftNumericalStats()

  val categoricalStats = new ThriftCategoricalStats()

  /** Class to emit the key/value bytes, created once per combiner */
  var emitter: MrContextEmitter[BytesWritable, BytesWritable] = null

  override def setup(context: CombinerContext): Unit = {
    emitter = MrContextEmitter(context)
  }

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: CombinerContext): Unit = {
    FactStatsCombiner.combine(FactStatsWritable.KeyState.getType(key), iter.iterator, numericalStats, categoricalStats, emitter, key, vout, serialiser)
  }
}

object FactStatsCombiner {

  val MaxCategories: Int = 100

  def combine(ty: FactStatsWritable.StatsType, iter: JIterator[BytesWritable], numericalStats: ThriftNumericalStats, categoricalStats: ThriftCategoricalStats,
              emitter: Emitter[BytesWritable, BytesWritable], kout: BytesWritable, vout: BytesWritable, serialiser: ThriftSerialiser): Unit = {
    ty match {
      case FactStatsWritable.Numerical =>
        combineNumerical(iter, numericalStats, serialiser)
        ThriftByteMutator.mutate(numericalStats, vout, serialiser)
      case FactStatsWritable.Categorical =>
        combineCategorical(iter, categoricalStats, serialiser)
        ThriftByteMutator.mutate(categoricalStats, vout, serialiser)
    }
    emitter.emit(kout, vout)
  }

  /** Combine all ThriftNumericalStats in `iter` and set the value to `numericalStats` */
  def combineNumerical(iter: JIterator[BytesWritable], numericalStats: ThriftNumericalStats, serialiser: ThriftSerialiser): Unit = {
    var count: Long = 0
    var sum: Double = 0.0
    var sqsum: Double = 0.0

    while(iter.hasNext) {
      val next = iter.next
      ThriftByteMutator.from(next, numericalStats, serialiser)
      count = count + numericalStats.getCount
      sum = sum + numericalStats.getSum
      sqsum = sqsum + numericalStats.getSqsum
    }
    numericalStats.setCount(count)
    numericalStats.setSum(sum)
    numericalStats.setSqsum(sqsum)
    ()
  }

  /** Combine all ThriftCategoricalStats in `iter`, stopping when the number of categories reaches 100, then set the value to `categoricalStats` */
  def combineCategorical(iter: JIterator[BytesWritable], categoricalStats: ThriftCategoricalStats, serialiser: ThriftSerialiser): Unit = {
    var categories: Int = 0
    val histogram: HashMap[String, Long] = new HashMap(MaxCategories)
    while(iter.hasNext) {
      val next = iter.next
      if(categories < MaxCategories) {
        ThriftByteMutator.from(next, categoricalStats, serialiser)
        categories = categories + categoricalStats.getCategories
        if(categories < MaxCategories) {
          categoricalStats.getHistogram.asScala.foreach(entry => {
            histogram.put(entry.getValue, if(histogram.containsKey(entry.getValue)) histogram.get(entry.getValue) + entry.getCount else entry.getCount)
          })
        }
      }
    }
    categoricalStats.setCategories(categories.toShort)
    categoricalStats.setHistogram(histogram.entrySet.asScala.toList.map(e => new ThriftHistogramEntry(e.getKey, e.getValue)).asJava)
    ()
  }
}

/**
 * Reducer for fact stats.
 *
 * This reducer takes stats grouped by factset id and date, combines the groups, then outputs json statistics
 */
class FactStatsReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, Text] {

  type ReducerContext = Reducer[BytesWritable, BytesWritable, NullWritable, Text]#Context

  /** Thrift deserialiser */
  val serialiser = ThriftSerialiser()

  /** Output value, created once per reducer and mutated per record */
  val vout = new Text

  val featureIdLookup = new FeatureIdLookup

  var featureNames: Array[String] = null

  val numericalStats = new ThriftNumericalStats()

  val categoricalStats = new ThriftCategoricalStats()

  /** Class to emit the key/value bytes, created once per mapper */
  var emitter: MrOutputEmitter[NullWritable, Text] = null

  var out: MultipleOutputs[NullWritable, Text] = null

  override def setup(context: ReducerContext): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, FactStatsJob.Keys.FeatureIdLookup, featureIdLookup)

    featureNames = FeatureLookups.sparseMapToArray(featureIdLookup.getIds.asScala.toList.map({ case (fid, idx) => (idx.toInt, fid) }), "")
    out = new MultipleOutputs(context)
    emitter = MrOutputEmitter(FactStatsJob.Keys.Out, out, "stats/part")
  }

  override def cleanup(context: ReducerContext): Unit =
    out.close()

  override def reduce(key: BytesWritable, iter: JIterable[BytesWritable], context: ReducerContext): Unit = {
    val feature: Int = FactStatsWritable.KeyState.getFeatureId(key)
    val date = Date.unsafeFromInt(FactStatsWritable.KeyState.getDate(key))
    val ty: FactStatsWritable.StatsType = FactStatsWritable.KeyState.getType(key)
    FactStatsReducer.reduce(ty, featureNames(feature), date, iter.iterator, numericalStats, categoricalStats, emitter, vout, serialiser)
    ()
  }
}

object FactStatsReducer {
  import argonaut._, Argonaut._

  type KeyInfo        = (String, Date)
  type Histogram      = Map[String,Long]
  type NumericalStats = (Long, Double, Double)
  type FactStatEncode = (KeyInfo, Either[NumericalStats, Histogram])

  def reduce(ty: FactStatsWritable.StatsType, feature: String, date: Date, iter: JIterator[BytesWritable], numericalStats: ThriftNumericalStats,
             categoricalStats: ThriftCategoricalStats, emitter: Emitter[NullWritable, Text], vout: Text, serialiser: ThriftSerialiser): Unit = {

    val key: KeyInfo = (feature, date)
    val encoded: Option[FactStatEncode] = ty match {
      case FactStatsWritable.Numerical =>
        FactStatsCombiner.combineNumerical(iter, numericalStats, serialiser)
        numericalEncode(key, numericalStats)
      case FactStatsWritable.Categorical =>
        FactStatsCombiner.combineCategorical(iter, categoricalStats, serialiser)
        categoricalEncode(key, categoricalStats)
    }
    encoded.foreach(e => {
      vout.set(e.asJson.nospaces)
      emitter.emit(NullWritable.get, vout)
    })
  }

  def numericalEncode(key: KeyInfo, numericalStats: ThriftNumericalStats): Option[FactStatEncode] =
    (numericalStats.count > 0).option({
      val mean: Double = numericalStats.getSum / numericalStats.getCount.toDouble
      (key, Left((numericalStats.getCount, mean, Math.sqrt(numericalStats.getSqsum / numericalStats.getCount.toDouble - mean * mean))))
    })

  def categoricalEncode(key: KeyInfo, categoricalStats: ThriftCategoricalStats): Option[FactStatEncode] =
    (categoricalStats.getCategories < FactStatsCombiner.MaxCategories).option(
      (key, Right(categoricalStats.getHistogram.asScala.map(e => (e.getValue, e.getCount)).toMap)))
}
