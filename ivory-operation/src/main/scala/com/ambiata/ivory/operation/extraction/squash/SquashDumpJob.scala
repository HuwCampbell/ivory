package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.EntityFilterLookup
import com.ambiata.ivory.mr.MrContextIvory
import com.ambiata.ivory.operation.extraction.SnapshotJob
import com.ambiata.ivory.storage.lookup.FeatureLookups
import com.ambiata.ivory.storage.metadata.{Metadata, SnapshotManifest}
import com.ambiata.mundane.control._
import com.ambiata.poacher.mr._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import scalaz._, effect.IO

object SquashDumpJob {

  def dump(repository: Repository, snapshotId: SnapshotId, output: IvoryLocation, features: List[FeatureId],
           entities: List[String]): RIO[Unit] = for {
    dictionary <- Metadata.latestDictionaryFromIvory(repository)
    filteredDct = if (features.isEmpty) dictionary else SquashDump.filterByConcreteOrVirtual(dictionary, features.toSet)
    lookup      = FeatureLookups.entityFilter(features.flatMap(SquashDump.lookupConcreteFromVirtual(dictionary, _)), entities)
    dateO      <- SnapshotManifest.fromIdentifier(repository, snapshotId).run
    date       <- ResultT.fromOption[IO, SnapshotManifest](dateO, s"Unknown snapshot ${snapshotId.render}").map(_.date)
    input       = Repository.snapshot(snapshotId)

    // When we're filtering, there's a very good chance we only need a reducer per concrete feature
    reducers    = dictionary.byConcrete.sources.size

    // HDFS below here
    hr         <- repository.asHdfsRepository[IO]
    job        <- initDumpJob(hr.configuration, date, hr.toIvoryLocation(input).toHdfsPath, filteredDct, lookup)
    out        <- output.asHdfsIvoryLocation[IO]
    _          <- SquashJob.run(job._1, job._2, reducers, filteredDct, out.toHdfsPath, hr.codec, SquashConfig.default, latest = false)
  } yield ()

  def initDumpJob(conf: Configuration, date: Date, input: Path, dictionary: Dictionary, lookup: EntityFilterLookup): RIO[(Job, MrContext)] = ResultT.safe {

    val job = Job.getInstance(conf)
    val ctx = MrContextIvory.newContext("ivory-squash-dump", job)

    job.getConfiguration.set(SnapshotJob.Keys.SnapshotDate, date.int.toString)
    ctx.thriftCache.push(job, Keys.Filter, lookup)

    // reducer
    job.setReducerClass(classOf[SquashReducerDump])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[Text])

    // input
    println(s"Input path: $input")
    MultipleInputs.addInputPath(job, input, classOf[SequenceFileInputFormat[_, _]], classOf[SquashMapperFilter])

    // output
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])

    (job, ctx)
  }

  object Keys {
    val Filter = ThriftCache.Key("squash-filter-lookup")
  }
}
