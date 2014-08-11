package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core.Prioritized
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.SnapshotJob
import com.ambiata.ivory.storage.fact.FactsetGlob
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.task.{BaseFactsPartitioner, FactsetJobKeys}
import com.ambiata.poacher.scoobi.ScoobiAction
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output._

object RenameJob {
  def run(mapping: RenameMapping, inputs: List[Prioritized[FactsetGlob]], target: Path, reducerLookups: ReducerLookups,
          codec: Option[CompressionCodec]): ScoobiAction[RenameStats] = for {
    conf  <- ScoobiAction.scoobiConfiguration
    job = Job.getInstance(conf.configuration)
    ctx = MrContext.newContext("ivory-rename", job)
    stats <- ScoobiAction.safe {

      job.setJarByClass(classOf[RenameReducer])
      job.setJobName(ctx.id.value)

      /* map */
      job.setMapOutputKeyClass(classOf[LongLongWritable])
      job.setMapOutputValueClass(classOf[BytesWritable])

      /* partition & sort */
      job.setPartitionerClass(classOf[LongIntFactsPartitioner])
      // Group by partition (namespace + date) so we can calculate the path String _once_
      job.setGroupingComparatorClass(classOf[LongWritable.Comparator])
      // Sort by everything, which includes the priority
      job.setSortComparatorClass(classOf[LongLongWritable.Comparator])

      /* reducer */
      job.setNumReduceTasks(reducerLookups.reducersNb)
      job.setReducerClass(classOf[RenameReducer])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[BytesWritable])

      /* input */
      val mappers = inputs.map(p => (classOf[RenameMapper], p.value))
      mappers.foreach({ case (clazz, factsetGlob) =>
        factsetGlob.paths.foreach { p =>
          println(s"Input path: ${p.path}")
          MultipleInputs.addInputPath(job, new Path(p.path), classOf[SequenceFileInputFormat[_, _]], clazz)
        }
      })

      /* output */
      LazyOutputFormat.setOutputFormatClass(job, classOf[SequenceFileOutputFormat[_, _]])
      MultipleOutputs.addNamedOutput(job, FactsetJobKeys.Out, classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable])
      FileOutputFormat.setOutputPath(job, ctx.output)

      /* compression */
      codec.foreach(cc => {
        Compress.intermediate(job, cc)
        Compress.output(job, cc)
      })

      /* cache / config initializtion */
      ctx.thriftCache.push(job, Keys.Mapping, RenameMapping.toThrift(mapping, reducerLookups.features))
      ctx.thriftCache.push(job, SnapshotJob.Keys.FactsetLookup, SnapshotJob.priorityTable(inputs))
      ctx.thriftCache.push(job, ReducerLookups.Keys.NamespaceLookup, reducerLookups.namespaces)
      ctx.thriftCache.push(job, ReducerLookups.Keys.ReducerLookup,   reducerLookups.reducers)
      ctx.thriftCache.push(job, SnapshotJob.Keys.FactsetVersionLookup, SnapshotJob.versionTable(inputs.map(_.value)))

      /* run job */
      if (!job.waitForCompletion(true))
        sys.error("ivory rename failed.")

      val group = job.getCounters.getGroup("ivory")
      RenameStats(group.findCounter(RenameJob.Keys.ReduceCounter).getValue)
    }
  _ <- ScoobiAction.fromHdfs(Committer.commit(ctx, {
      case "factset" => target
    }, true))
  } yield stats

  object Keys {
    val Mapping = ThriftCache.Key("ivory.rename.mapping")
    val MapCounter = "rename-map"
    val ReduceCounter = "rename-reduce"
  }
}

class LongIntFactsPartitioner extends BaseFactsPartitioner[LongLongWritable] {
  def get(k: LongLongWritable): Long = k.l1
}
