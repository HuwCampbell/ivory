package com.ambiata.ivory.operation.rename

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.MrContextIvory
import com.ambiata.ivory.operation.extraction.{SnapshotJob, IvoryInputs}
import com.ambiata.ivory.storage.lookup.{ReducerLookups, FactsetLookups}
import com.ambiata.ivory.storage.task.FactsetJobKeys
import com.ambiata.ivory.storage.plan._
import com.ambiata.poacher.mr._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output._
import scalaz._, Scalaz._

object RenameJob {
  def run(repository: HdfsRepository, mapping: RenameMapping, plan: RenamePlan, target: Path, reducerLookups: ReducerLookups): RIO[RenameStats] = for {
    job <- Job.getInstance(repository.configuration).pure[RIO]
    ctx = MrContextIvory.newContext("ivory-rename", job)
    stats <- RIO.safe {

      job.setJarByClass(classOf[RenameReducer])
      job.setJobName(ctx.id.value)

      /* map */
      job.setMapOutputKeyClass(classOf[BytesWritable])
      job.setMapOutputValueClass(classOf[BytesWritable])

      /* partition & sort */
      job.setPartitionerClass(classOf[RenameWritable.PartitionerFeatureId])
      // Group by partition featureId + date so we can calculate the path String _once_
      job.setGroupingComparatorClass(classOf[RenameWritable.GroupingByFeatureIdDate])
      // Sort by everything, which includes the priority
      job.setSortComparatorClass(classOf[RenameWritable.ComparatorFeatureIdDateEntityTimePriority])

      /* reducer */
      job.setNumReduceTasks(reducerLookups.reducersNb)
      job.setReducerClass(classOf[RenameReducer])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[BytesWritable])

      /* input */
      IvoryInputs.configure(ctx, job, repository, plan.datasets, {
        case FactsetFormat.V1 => classOf[RenameV1Mapper]
        case FactsetFormat.V2 => classOf[RenameV2Mapper]
      }, _ => Crash.error(Crash.Invariant, "Rename should not be run on a snapshot!"))

      /* output */
      LazyOutputFormat.setOutputFormatClass(job, classOf[SequenceFileOutputFormat[_, _]])
      MultipleOutputs.addNamedOutput(job, FactsetJobKeys.Out, classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable])
      FileOutputFormat.setOutputPath(job, ctx.output)

      /* compression */
      repository.codec.foreach(cc => {
        Compress.intermediate(job, cc)
        Compress.output(job, cc)
      })

      /* cache / config initializtion */
      ctx.thriftCache.push(job, Keys.Mapping, RenameMapping.toThrift(mapping, reducerLookups.features))
      ctx.thriftCache.push(job, SnapshotJob.Keys.FactsetLookup, FactsetLookups.priorityTable(plan.datasets))
      ctx.thriftCache.push(job, ReducerLookups.Keys.NamespaceLookup, reducerLookups.namespaces)
      ctx.thriftCache.push(job, ReducerLookups.Keys.ReducerLookup,   reducerLookups.reducers)
      ctx.thriftCache.push(job, SnapshotJob.Keys.FactsetVersionLookup, FactsetLookups.versionTable(plan.datasets))

      /* run job */
      if (!job.waitForCompletion(true))
        Crash.error(Crash.RIO, "ivory rename failed.")

      val group = job.getCounters.getGroup("ivory")
      RenameStats(group.findCounter(RenameJob.Keys.ReduceCounter).getValue)
    }
  _ <- Committer.commit(ctx, {
      case "factset" => target
    }, true).run(repository.configuration)
  } yield stats

  object Keys {
    val Mapping = ThriftCache.Key("ivory.rename.mapping")
    val MapCounter = "rename-map"
    val ReduceCounter = "rename-reduce"
  }
}
