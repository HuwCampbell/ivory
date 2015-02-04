package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.EntityFilterLookup
import com.ambiata.ivory.mr.MrContextIvory
import com.ambiata.ivory.operation.extraction.{IvoryInputs, SnapshotJob}
import com.ambiata.ivory.storage.lookup.FeatureLookups
import com.ambiata.ivory.storage.manifest.SnapshotManifest
import com.ambiata.ivory.storage.metadata.{Metadata, SnapshotStorage}
import com.ambiata.mundane.control._
import com.ambiata.poacher.mr._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{TextOutputFormat, MultipleOutputs}
import scalaz._, Scalaz._, effect.IO

object SquashDumpJob {

  def dump(repository: Repository, snapshotId: SnapshotId, output: IvoryLocation, features: List[FeatureId],
           entities: List[String]): RIO[Unit] = for {
    dictionary <- Metadata.latestDictionaryFromIvory(repository)
    filteredDct <-
      if (features.isEmpty) RIO.ok(dictionary)
      else RIO.fromDisjunctionString(SquashDump.filterByConcreteOrVirtual(dictionary, features.toSet))
    lookup      = FeatureLookups.entityFilter(features.flatMap(SquashDump.lookupConcreteFromVirtual(dictionary, _)), entities)
    snapshot   <- SnapshotStorage.byIdOrFail(repository, snapshotId)
    // When we're filtering, there's a very good chance we only need a reducer per concrete feature
    reducers    = filteredDct.byConcrete.sources.size

    // HDFS below here
    hr         <- repository.asHdfsRepository
    job        <- initDumpJob(hr, snapshot.date, snapshot, filteredDct, lookup)
    out        <- output.asHdfsIvoryLocation
    _          <- SquashJob.run(job._1, job._2, reducers, filteredDct, out.toHdfsPath, hr.codec, SquashConfig.default, latest = false)
  } yield ()

  def initDumpJob(repo: HdfsRepository, date: Date, snapshot: Snapshot, dictionary: Dictionary, lookup: EntityFilterLookup): RIO[(Job, MrContext)] = for {
    inputs <- SnapshotStorage.location(repo, snapshot).traverse(_.asHdfsIvoryLocation.map(_.toHdfsPath))
    ret    <- RIO.safe {
      val job = Job.getInstance(repo.configuration)
      val ctx = MrContextIvory.newContext("ivory-squash-dump", job)
  
      job.getConfiguration.set(SnapshotJob.Keys.SnapshotDate, date.int.toString)
      ctx.thriftCache.push(job, Keys.Filter, lookup)
  
      // reducer
      job.setReducerClass(classOf[SquashReducerDump])
      job.setOutputKeyClass(classOf[NullWritable])
      job.setOutputValueClass(classOf[Text])
  
      // input
      inputs.foreach(input => {
        println(s"Input path: $input")
        MultipleInputs.addInputPath(job, input, classOf[SequenceFileInputFormat[_, _]], classOf[SquashMapperFilter])
      })
  
      // output
      MultipleOutputs.addNamedOutput(job, Keys.Out, classOf[TextOutputFormat[_, _]],  classOf[NullWritable], classOf[Text])
  
      (job, ctx)
    }
  } yield ret

  object Keys {
    val Filter = ThriftCache.Key("squash-filter-lookup")
    val Out = "dumpOut"
  }
}
