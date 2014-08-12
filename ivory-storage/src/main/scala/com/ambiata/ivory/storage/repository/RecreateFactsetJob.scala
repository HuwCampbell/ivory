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
import com.ambiata.ivory.storage.task.FactsetJob
import com.ambiata.mundane.io.{BytesQuantity, FilePath}
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, LazyOutputFormat, MultipleOutputs, SequenceFileOutputFormat}

import scalaz.{Reducer => _, _}

/**
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
    val ctx = FactsetJob.configureJob("ivory-recreate-factset", job, dictionary, reducerLookups, partitions, target, codec)

    /** input */
    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])

    /** map */
    job.setMapperClass(classOf[RecreateFactsetMapper])
    job.getConfiguration.set(Keys.Version, version.toString)

    /** run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory recreate factset failed.")

    /** commit files to factset */
    Committer.commit(ctx, {
      case "factset" => target
    }, true).run(conf).run.unsafePerformIO
  }

  object Keys {
    val Version = "version"
  }
}

class RecreateFactsetMapper extends Mapper[NullWritable, BytesWritable, LongWritable, BytesWritable] {
  /** Factset version */
  var version = "2"

  /** Context object holding dist cache paths */
  var ctx: MrContext = null

  /** The output key, only create once per mapper. */
  val kout = new LongWritable

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /** Partition created from input split path, only created once per mapper */
  var partition: Partition = null

  /** Input split path, only created once per mapper */
  var path: FilePath = null

  var featureIdLookup = new FeatureIdLookup

  var createFact: ThriftFact => Fact = null

  val thrift = new ThriftFact

  override def setup(context: MapperContext): Unit = {
    version = context.getConfiguration.get(Keys.Version, "")
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    path = FilePath(MrContext.getSplitPath(context.getInputSplit).toString)
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.FeatureIdLookup, featureIdLookup)
    partition = Partition.parseFile(path) match {
      case Success(p) => p
      case Failure(e) => sys.error(s"Can not parse partition $e")
    }

    createFact =
      if (FactsetVersion.fromString(version) == Some(FactsetVersionTwo))
        (tfact: ThriftFact) => PartitionFactThriftStorageV2.createFact(partition, tfact)
      else
        (tfact: ThriftFact) => PartitionFactThriftStorageV1.createFact(partition, tfact)
  }

  val serializer = ThriftSerialiser()

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    serializer.fromBytesViewUnsafe(thrift, value.getBytes, 0, value.getLength)
    val fact = createFact(thrift)
    val k = featureIdLookup.ids.get(fact.featureId.toString).toInt

    kout.set((k.toLong << 32) | fact.date.int.toLong)

    val v = serializer.toBytes(fact.toThrift)
    vout.set(v, 0, v.length)

    context.write(kout, vout)
  }

}

object RecreateFactsetMapper {
  type MapperContext = Mapper[NullWritable, BytesWritable, LongWritable, BytesWritable]#Context
}
