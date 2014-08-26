package com.ambiata.ivory.storage.task

import com.ambiata.ivory.core.{Date, Dictionary, Crash}
import com.ambiata.ivory.core.thrift.DictionaryThriftConversion
import com.ambiata.ivory.lookup.NamespaceLookup
import com.ambiata.ivory.mr.{ThriftCache, Committer, Compress, MrContext}
import com.ambiata.ivory.storage.fact.FactsetVersion
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.repository.RecreateFactsetMapper
import com.ambiata.mundane.io.BytesQuantity
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, BytesWritable, LongWritable}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, MultipleOutputs, SequenceFileOutputFormat, LazyOutputFormat}

import scalaz.{\/-, -\/}

/**
 * This object factors the map reducer configuration for both the ingest and recreate-factset tasks.
 *
 * It configures a MapReduce job where mappers will output
 *
 * (LongWritable, BytesWritable)
 *
 * Where LongWritable represents the key of a fact
 * and BytesWritable is the Thrift encoded fact
 *
 * The Partitioner/Reducer for this job are FactsPartitioner and FactsReducer.
 *   the partitioner orders fact according to their key
 *   the partitioner groups the facts so that each reducer will receive the same amount of data
 *     (thus determining the number of reducers, given by reducerLookups)
 *
 *   the reducer outputs the facts in sequence files in partitions that are given
 *   by a lookup table in reducerLookups
 *
 */
object FactsetJob {

  def configureJob(name: String, job: Job, dictionary: Dictionary, reducerLookups: ReducerLookups, inputPaths: List[Path], targetPath: Path, codec: Option[CompressionCodec]): MrContext = {
    val ctx = MrContext.newContext(name, job)

    job.setJarByClass(classOf[FactsPartitioner])
    job.setJobName(ctx.id.value)

    /** inputs */
    // job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]]) must be done by the caller method
    FileInputFormat.addInputPaths(job, inputPaths.mkString(","))

    /** map */
    // job.setMapperClass(....) must be done by the caller method
    job.setMapOutputKeyClass(classOf[LongWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    /** partition & sort */
    job.setPartitionerClass(classOf[FactsPartitioner])
    job.setGroupingComparatorClass(classOf[LongWritable.Comparator])
    job.setSortComparatorClass(classOf[LongWritable.Comparator])

    /** reducer */
    job.setNumReduceTasks(reducerLookups.reducersNb)
    job.setReducerClass(classOf[FactsReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    /** output */
    LazyOutputFormat.setOutputFormatClass(job, classOf[SequenceFileOutputFormat[_, _]])
    MultipleOutputs.addNamedOutput(job, FactsetJobKeys.Out, classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable])
    FileOutputFormat.setOutputPath(job, ctx.output)

    /** compression */
    codec.foreach { cc =>
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    }

    /** cache / config initialization */
    ctx.thriftCache.push(job, ReducerLookups.Keys.NamespaceLookup, reducerLookups.namespaces)
    ctx.thriftCache.push(job, ReducerLookups.Keys.FeatureIdLookup, reducerLookups.features)
    ctx.thriftCache.push(job, ReducerLookups.Keys.ReducerLookup,   reducerLookups.reducers)
    val thriftDict = DictionaryThriftConversion.dictionaryToThrift(dictionary)
    ctx.thriftCache.push(job, ReducerLookups.Keys.Dictionary, thriftDict)
    ctx
  }
}

object FactsetJobKeys {
  val Out = "out"
}
