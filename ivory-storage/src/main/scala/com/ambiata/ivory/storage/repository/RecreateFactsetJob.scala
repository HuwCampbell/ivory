package com.ambiata.ivory
package storage
package repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.storage.fact.FeatureIdIndexOption
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.repository.RecreateFactsetJob.Keys
import com.ambiata.ivory.storage.repository.RecreateFactsetMapper._
import com.ambiata.ivory.storage.task.{FactsetWritable, FactsetJob}
import com.ambiata.ivory.storage.partition._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.{BytesQuantity, FilePath}
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}

import scalaz.{Reducer => _, _}
import scala.collection.JavaConverters._

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
  def run(repository: HdfsRepository, dictionary: Dictionary, namespaces: List[(Namespace, BytesQuantity)],
          factset: Factset, target: Path, reducerSize: BytesQuantity): RIO[Unit] = {
    val reducerLookups = ReducerLookups.createLookups(dictionary, namespaces, reducerSize)
    for {
      reducers <-
        if (reducerLookups.reducersNb != 0)
          RIO.safe(reducerLookups.reducersNb)
        else
          Hdfs.size(repository.toIvoryLocation(Repository.factset(factset.id)).toHdfsPath).run(repository.configuration)
            .map(_.toBytes.value / reducerSize.toBytes.value).map(l => math.max(1, l.toInt))
      job = Job.getInstance(repository.configuration)
      ctx = FactsetJob.configureJob("ivory-recreate-factset", job, dictionary,
        reducerLookups.copy(reducersNb = reducers), target, repository.codec)
      _ <- RIO.io {
        /** input */
        val partitions = Partitions.globs(repository, factset.id, factset.partitions.map(_.value))
        job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])
        FileInputFormat.addInputPaths(job, partitions.mkString(","))

        /** map */
        job.setMapperClass(classOf[RecreateFactsetMapper])
        job.getConfiguration.set(Keys.Version, factset.format.toStringFormat)

        /** run job */
        if (!job.waitForCompletion(true))
          Crash.error(Crash.RIO, "ivory recreate factset failed.")
      }

      /** commit files to factset */
      _ <- Committer.commit(ctx, {
        case "factset" => target
      }, true).run(repository.configuration)
    } yield ()
  }

  object Keys {
    val Version = "version"
  }
}

class RecreateFactsetMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  /** Factset version */
  var version = "2"

  /** Context object holding dist cache paths */
  var ctx: MrContext = null

  /** The output key, only create once per mapper. */
  val kout = FactsetWritable.create

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /** Partition created from input split path, only created once per mapper */
  var partition: Partition = null

  /** Input split path, only created once per mapper */
  var path: FilePath = null

  var featureIdLookup = new FeatureIdLookup
  var featureIdMissing = FeatureIdIndex(0)

  var createFact: ThriftFact => Fact = null

  val thrift = new ThriftFact

  override def setup(context: MapperContext): Unit = {
    version = context.getConfiguration.get(Keys.Version, "")
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    path = FilePath.unsafe(MrContext.getSplitPath(context.getInputSplit).toString)
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.FeatureIdLookup, featureIdLookup)
    featureIdMissing = FeatureIdIndex(featureIdLookup.ids.asScala.values.max + 1)
    partition = Partition.parseFile(path) match {
      case Success(p) => p
      case Failure(e) => Crash.error(Crash.Serialization, s"Can not parse partition $e")
    }
    createFact = PartitionFactThriftStorage.createFact(partition, _)
  }

  val serializer = ThriftSerialiser()

  override def map(key: NullWritable, value: BytesWritable, context: MapperContext): Unit = {
    serializer.fromBytesViewUnsafe(thrift, value.getBytes, 0, value.getLength)
    val fact = createFact(thrift)
    val featureId = FeatureIdIndexOption.lookup(fact, featureIdLookup)

    // If we don't know about this feature any more just default to a different value - don't fail
    FactsetWritable.set(fact, kout, if (featureId.isDefined) featureId.get else featureIdMissing)

    val v = serializer.toBytes(fact.toThrift)
    vout.set(v, 0, v.length)

    context.write(kout, vout)
  }

}

object RecreateFactsetMapper {
  type MapperContext = Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context
}
