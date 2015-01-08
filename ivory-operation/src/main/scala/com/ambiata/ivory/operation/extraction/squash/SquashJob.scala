package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.{FeatureIdLookup, FeatureReduction, FeatureReductionLookup}
import com.ambiata.ivory.mr.MrContextIvory
import com.ambiata.ivory.operation.extraction.{ChordJob, Snapshots, SnapshotJob}
import com.ambiata.ivory.storage.entities._
import com.ambiata.ivory.storage.lookup.{FeatureLookups, ReducerLookups, ReducerSize, WindowLookup}
import com.ambiata.ivory.storage.manifest.{SnapshotExtractManifest, SnapshotManifest}
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FileName
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.notion.core._
import com.ambiata.poacher.mr._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import scala.collection.JavaConverters._
import scalaz._, Scalaz._, effect.IO

object SquashJob {

  def squashFromSnapshotWith[A](repository: Repository, snapmeta: SnapshotMetadata, conf: SquashConfig,
                                cluster: Cluster): RIO[(ShadowOutputDataset, Dictionary)] = for {
    // FIX this isn't ideal, but reflects the fact the snapshot doesn't really pass enough precise information
    //     to handle this (and is related to the fact that it is possible for this to be run at _not_ the
    //     latest commit incorrectly). Currently this is just taking the latest commit, which _should_ be correct
    //     and with upcoming changes like #427 this should then become always correct.
    commitId   <- Metadata.findOrCreateLatestCommitId(repository)
    commit     <- CommitStorage.byIdOrFail(repository, commitId)
    dictionary =  commit.dictionary.value
    hdfsIvoryL <- repository.toIvoryLocation(Repository.snapshot(snapmeta.id)).asHdfsIvoryLocation
    in          = ShadowOutputDataset.fromIvoryLocation(hdfsIvoryL)
    job        <- SquashJob.initSnapshotJob(cluster.hdfsConfiguration, snapmeta.date)
    result     <- squash(repository, dictionary, in, conf, job, cluster)
    _          <- SnapshotExtractManifest.io(cluster.toIvoryLocation(result.location)).write(SnapshotExtractManifest.create(commitId, snapmeta.id))
  } yield result -> dictionary

  /**
   * Returns the path to the squashed facts if we have any virtual features (and true), or the original `in` path (and false).
   * The resulting path can/should be deleted after use
   *
   * There are two possible date inputs for squash:
   *
   * 1. From a snapshot where all facts can be windowed by a single date.
   *    This is the only mode that Ivory currently supports.
   * 2. From a chord, where a single entity may have one or more dates. It will be important to pre-calculate a starting
   *    date for every possible entity date, and then look that up per entity on the reducer.
   */
  def squash(repository: Repository, dictionary: Dictionary, input: ShadowOutputDataset, conf: SquashConfig,
             job: (Job, MrContext), cluster: Cluster): RIO[ShadowOutputDataset] = for {
    // This is about the best we can do at the moment, until we have more size information about each feature
    rs     <- ReducerSize.calculate(input.hdfsPath, 1.gb).run(cluster.hdfsConfiguration)
    _      <- initJob(job._1, input.hdfsPath)
    key    <- Repository.tmpDir("squash")
    hr     <- repository.asHdfsRepository
    shadow = ShadowOutputDataset.fromIvoryLocation(hr.toIvoryLocation(key))
    prof   <- run(job._1, job._2, rs, dictionary, shadow.hdfsPath, cluster.codec, conf, latest = true)
    _      <- IvoryLocation.writeUtf8Lines(hr.toIvoryLocation(key) </> FileName.unsafe(".profile"), SquashStats.asPsvLines(prof))
  } yield shadow

  def initSnapshotJob(conf: Configuration, date: Date): RIO[(Job, MrContext)] = RIO.safe {
    val job = Job.getInstance(conf)
    val ctx = MrContextIvory.newContext("ivory-squash-snapshot", job)
    job.setReducerClass(classOf[SquashReducerSnapshot])
    job.getConfiguration.set(SnapshotJob.Keys.SnapshotDate, date.int.toString)
    (job, ctx)
  }

  def initChordJob(conf: Configuration, chord: Entities): RIO[(Job, MrContext)] = RIO.safe {
    val job = Job.getInstance(conf)
    val ctx = MrContextIvory.newContext("ivory-squash-chord", job)
    job.setReducerClass(classOf[SquashReducerChord])
    ctx.thriftCache.push(job, ChordJob.Keys.ChordEntitiesLookup, Entities.toChordEntities(chord))
    (job, ctx)
  }

  def initJob(job: Job, input: Path): RIO[Unit] = RIO.safe[Unit] {
    // reducer
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    // input
    println(s"Input path: $input")
    MultipleInputs.addInputPath(job, input, classOf[SequenceFileInputFormat[_, _]], classOf[SquashMapper])

    // output
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
  }

  def run(job: Job, ctx: MrContext, reducers: Int, dict: Dictionary, output: Path, codec: Option[CompressionCodec],
          squashConf: SquashConfig, latest: Boolean): RIO[SquashStats] = {

    job.setJarByClass(classOf[SquashPartitioner])
    job.setJobName(ctx.id.value)

    // map
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    // partition & sort
    job.setPartitionerClass(classOf[SquashPartitioner])
    job.setGroupingComparatorClass(classOf[SquashWritable.GroupingByFeatureId])
    job.setSortComparatorClass(classOf[SquashWritable.ComparatorFeatureId])

    job.setNumReduceTasks(reducers)

    val tmpout = new Path(ctx.output, "squash")
    FileOutputFormat.setOutputPath(job, tmpout)

    // compression
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    // cache / config initialization
    val dictionary = dict.byConcrete
    job.getConfiguration.setInt(Keys.ProfileMod, squashConf.profileSampleRate)
    val (featureIdLookup, reductionLookup) = dictToLookup(dictionary, latest)
    ctx.thriftCache.push(job, SnapshotJob.Keys.FeatureIdLookup, featureIdLookup)
    ctx.thriftCache.push(job, ReducerLookups.Keys.ReducerLookup,
      SquashReducerLookup.create(dictionary, featureIdLookup, reducers))

    ctx.thriftCache.push(job, Keys.ExpressionLookup, reductionLookup)
    ctx.thriftCache.push(job, Keys.FeatureIsSetLookup, FeatureLookups.isSetTableConcrete(dictionary))

    // run job
    if (!job.waitForCompletion(true))
      Crash.error(Crash.RIO, "ivory squash failed.")

    // commit files to factset
    Committer.commit(ctx, {
      case "squash" => output
    }, true).run(job.getConfiguration).as {
      def update(groupName: String)(f: Long => SquashCounts): Map[String, SquashCounts] = {
        val group = job.getCounters.getGroup(groupName)
        group.iterator().asScala.map(c => c.getName -> f(c.getValue)).toMap
      }
      SquashStats(
        update(Keys.CounterTotalGroup)(c => SquashCounts(c, 0, 0, 0)) |+|
        update(Keys.CounterSaveGroup)(c => SquashCounts(0, c, 0, 0)) |+|
        update(Keys.ProfileTotalGroup)(c => SquashCounts(0, 0, c, 0)) |+|
        update(Keys.ProfileSaveGroup)(c => SquashCounts(0, 0, 0, c))
      )
    }
  }

  def dictToLookup(dictionary: DictionaryConcrete, latest: Boolean): (FeatureIdLookup, FeatureReductionLookup) = {
    val featureIdLookup = new FeatureIdLookup
    val reductionLookup = new FeatureReductionLookup
    dictionary.sources.foreach { case (fid, cg) =>
      dictionary.byFeatureIndexReverse.get(fid).map { i =>
        featureIdLookup.putToIds(fid.toString, i)
        reductionLookup.putToReductions(i, concreteGroupToReductions(fid, cg, latest).asJava)
      }
    }
    (featureIdLookup, reductionLookup)
  }

  def concreteGroupToReductions(fid: FeatureId, cg: ConcreteGroup, latest: Boolean): List[FeatureReduction] = {
    // We use 'latest' reduction to output the concrete feature as well
    // NOTE: Only states have the concept of "latest" - it doesn't make sense for sets
    val cr = (latest && cg.definition.mode.isState).option(reductionToThriftExp(fid, Query.empty, cg.definition.encoding, None))
    val vrs = cg.virtual.map((reductionToThrift(cg.definition.encoding) _).tupled)
    cr.toList ++ vrs
  }

  def reductionToThrift(encoding: Encoding)(fid: FeatureId, vd: VirtualDefinition): FeatureReduction =
    reductionToThriftExp(fid, vd.query, encoding, vd.window)

  def reductionToThriftExp(fid: FeatureId, query: Query, encoding: Encoding, window: Option[Window]): FeatureReduction = {
    val fr = new FeatureReduction(fid.namespace.name, fid.toString, fid.name, Expression.asString(query.expression),
      Encoding.render(encoding), WindowLookup.toInt(window))
    query.filter.map(_.render).foreach(fr.setFilter)
    fr
  }

  object Keys {
    val ProfileMod = "squash-profile-mod"
    val CounterTotalGroup = "squash-counter-total"
    val CounterSaveGroup = "squash-counter-save"
    val ProfileTotalGroup = "squash-profile-total"
    val ProfileSaveGroup = "squash-profile-save"
    val ExpressionLookup = ThriftCache.Key("squash-expression-lookup")
    val FeatureIsSetLookup = ThriftCache.Key("feature-is-set-lookup")
  }
}
