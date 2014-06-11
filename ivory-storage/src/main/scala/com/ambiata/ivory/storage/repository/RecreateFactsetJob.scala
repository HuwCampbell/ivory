package com.ambiata.ivory
package storage
package repository

import java.lang.{Iterable => JIterable}

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.mr._
import com.ambiata.ivory.storage.fact.{FactsetVersion, FactsetVersionTwo}
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.repository.RecreateFactsetJob.Keys
import com.ambiata.ivory.storage.repository.RecreateFactsetMapper._
import com.ambiata.mundane.io.BytesQuantity
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, LazyOutputFormat, MultipleOutputs, SequenceFileOutputFormat}
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TDeserializer, TSerializer}

import scalaz.{Reducer => _, _}

/*
 * This is a hand-coded MR job to read facts from factsets as
 * Thrift records and write them out again, sorted and compressed
 */
object RecreateFactsetJob {

  /**
   * Run the MR job
   *
   * In order to reduce the amount of transferred data between the mappers and the reducers we use
   * the dictionary and create lookup tables for each fact
   *
   * 
   *
   *
   */
  def run(conf: Configuration, version: FactsetVersion, dictionary: Dictionary, namespaces: List[(String, BytesQuantity)], partitions: List[Path], target: Path, reducerSize: BytesQuantity, codec: Option[CompressionCodec]): Unit = {
    val reducerLookups = ReducerLookups.createLookups(dictionary, namespaces, reducerSize)

    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("ivory-recreate-factset", job)

    job.setJarByClass(classOf[RecreateFactsetMapper])
    job.setJobName(ctx.id.value)

    /* map */
    job.setMapperClass(classOf[RecreateFactsetMapper])
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    /* partition & sort */
    job.setPartitionerClass(classOf[RecreateFactsetPartitioner])
    job.setGroupingComparatorClass(classOf[LongWritable.Comparator])
    job.setSortComparatorClass(classOf[LongWritable.Comparator])

    /* reducer */
    job.setNumReduceTasks(reducerLookups.reducersNb)
    job.setReducerClass(classOf[RecreateFactsetReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    /* input */
    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])
    FileInputFormat.addInputPaths(job, partitions.mkString(","))

    /* output */
    LazyOutputFormat.setOutputFormatClass(job, classOf[SequenceFileOutputFormat[_, _]])
    MultipleOutputs.addNamedOutput(job, Keys.Out, classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable])
    FileOutputFormat.setOutputPath(job, ctx.output)
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])

    FileOutputFormat.setOutputPath(job, ctx.output)
    job.getConfiguration.set(Keys.Version, version.toString)

    /* compression */
    codec.foreach { cc =>
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    }

    /* cache / config initialization */
    ctx.thriftCache.push(job, ReducerLookups.Keys.NamespaceLookup, reducerLookups.namespaces)
    ctx.thriftCache.push(job, ReducerLookups.Keys.FeatureIdLookup, reducerLookups.features)
    ctx.thriftCache.push(job, ReducerLookups.Keys.ReducerLookup,   reducerLookups.reducers)
    ctx.thriftCache.push(job, ReducerLookups.Keys.Dictionary,      DictionaryThriftConversion.dictionary.to(dictionary))

    /* run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory recreate factset failed.")

    /* commit files to factset */
    Committer.commit(ctx, {
      case path => new Path(target, path)
    }, true).run(conf).run.unsafePerformIO
  }

  def partitionFor(lookup: NamespaceLookup, key: LongWritable): String =
    "factset" + "/" + lookup.namespaces.get((key.get >>> 32).toInt) + "/" + Date.unsafeFromInt((key.get & 0xffffffff).toInt).slashed + "/part"

  object Keys {
    val Version = "version"
    val Out = "out"
    val FactsetLookup = ThriftCache.Key("factset-lookup")
  }
}

class RecreateFactsetMapper extends Mapper[NullWritable, BytesWritable, LongWritable, BytesWritable] {
  /* Factset version */
  var version = "2"

  /* Context object holding dist cache paths */
  var ctx: MrContext = null

  /* The output key, only create once per mapper. */
  val kout = new LongWritable

  /* The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /* Partition created from input split path, only created once per mapper */
  var partition: Partition = null

  /* Input split path, only created once per mapper */
  var stringPath: String = null
  
  var featureIdLookup = new FeatureIdLookup

  var parseFact: ThriftFact => ParseError \/ Fact = null

  val thrift = new ThriftFact

  override def setup(context: MapperContext): Unit = {
    version = context.getConfiguration.get(Keys.Version, "")
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    stringPath = MrContext.getSplitPath(context.getInputSplit).toString
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.FeatureIdLookup, featureIdLookup)
    partition = Partition.parseWith(stringPath) match {
      case Success(p) => p
      case Failure(e) => sys.error(s"Can not parse partition $e")
    }

    parseFact =
      if (FactsetVersion.fromString(version) == Some(FactsetVersionTwo)) PartitionFactThriftStorageV2.parseFact(partition)
      else                                                               PartitionFactThriftStorageV1.parseFact(partition)
  }

  /** Thrift deserializer. */
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)
  /* Value serializer. */
  val serializer = new TSerializer(new TCompactProtocol.Factory)

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    deserializer.deserialize(thrift, value.copyBytes())
    parseFact(thrift).toOption.foreach { fact =>
      val k = featureIdLookup.ids.get(fact.featureId.toString).toInt

      kout.set((k.toLong << 32) | fact.date.int.toLong)

      val v = serializer.serialize(fact.toThrift)
      vout.set(v, 0, v.length)

      context.write(kout, vout)
    }
  }

}

object RecreateFactsetMapper {
  type MapperContext = Mapper[NullWritable, BytesWritable, LongWritable, BytesWritable]#Context
}

/**
 * Partitioner for ivory-recreate.
 *
 * Keys are partitioned by the externalized feature id (held in the top 32 bits of the key)
 * into predetermined buckets. We use the predetermined buckets as upfront knowledge of
 * the input size is used to reduce skew on input data.
 */
class RecreateFactsetPartitioner extends Partitioner[LongWritable, BytesWritable] with Configurable {
  var conf: Configuration = null
  var ctx: MrContext = null
  val reducerLookup = new ReducerLookup

  def setConf(c: Configuration): Unit = {
    conf = c
    ctx = MrContext.fromConfiguration(conf)
    ctx.thriftCache.pop(conf, ReducerLookups.Keys.ReducerLookup, reducerLookup)
  }

  def getConf: Configuration =
    conf

  def getPartition(k: LongWritable, v: BytesWritable, partitions: Int): Int =
    reducerLookup.reducers.get((k.get >>> 32).toInt) % partitions
}

/*
 * Reducer for ivory-ingest.
 *
 * This is an almost a pass through, most of the work is done via partition & sort.
 *
 * The input key is a long, where the top 32 bits is an externalized feature id that we can
 * use to lookup the namespace, and the bottom 32 bits is an ivory date representation that we
 * can use to determine the partition to write out to.
 *
 * The input value is the bytes representation of the fact ready to write out.
 *
 * The output is a sequence file, with no key, and the bytes of the serialized Fact. The output
 * is partitioned by namespace and date (determined by the input key).
 */
class RecreateFactsetReducer extends Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable] {
  var ctx: MrContext = null
  var out: MultipleOutputs[NullWritable, BytesWritable] = null
  var namespaceLookup: NamespaceLookup = new NamespaceLookup

  override def setup(context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.NamespaceLookup, namespaceLookup)
    out = new MultipleOutputs(context)
  }

  override def cleanup(context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit =
    out.close()

  override def reduce(key: LongWritable, iter: JIterable[BytesWritable], context: Reducer[LongWritable, BytesWritable, NullWritable, BytesWritable]#Context): Unit = {
    val path = ReducerLookups.factsetPartitionFor(namespaceLookup, key)
    val iterator = iter.iterator
    while (iterator.hasNext)
      out.write(Keys.Out, NullWritable.get, iterator.next, path)
  }
}

