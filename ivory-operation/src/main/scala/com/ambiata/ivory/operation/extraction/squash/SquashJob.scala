package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.{FeatureIdLookup, FeatureReduction, FeatureReductionLookup}
import com.ambiata.ivory.operation.extraction.SnapshotJob
import com.ambiata.ivory.operation.extraction.snapshot._
import com.ambiata.ivory.storage.lookup.{ReducerLookups, ReducerSize}
import com.ambiata.ivory.storage.metadata.SnapshotManifest
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
import IvorySyntax._
import scala.collection.JavaConverters._
import scalaz._, Scalaz._

object SquashJob {

  def squashFromSnapshotWith[A](repository: Repository, dictionary: Dictionary, snapmeta: SnapshotManifest,
                                output: IvoryLocation, conf: SquashConfig)(f: Key => ResultTIO[A]): ResultTIO[A] = for {
    toSquash        <- squash(repository, dictionary, Repository.snapshot(snapmeta.snapshotId), snapmeta.date, conf)
    (profile, key, doSquash) =  toSquash
    a               <- f(key)
    _               <- ResultT.when(doSquash, for {
      _             <- profile.traverseU {
        prof => IvoryLocation.writeUtf8Lines(output </> FileName.unsafe(".profile"), SquashStats.asPsvLines(prof))
      }
      _             <- repository.store.deleteAll(key)
    } yield ())
  } yield a

  /**
   * Returns the path to the squashed facts if we have any virtual features (and true), or the original `in` path (and false).
   * The resulting path can/should be deleted after use (if there was a squash).
   *
   * There are two possible date inputs for squash:
   *
   * 1. From a snapshot where all facts can be windowed by a single date.
   *    This is the only mode that Ivory currently supports.
   * 2. From a chord, where a single entity may have one or more dates. It will be important to pre-calculate a starting
   *    date for every possible entity date, and then look that up per entity on the reducer.
   *    This is _not_ implemented yet.
   */
  def squash(repository: Repository, dictionary: Dictionary, input: Key, date: Date, conf: SquashConfig): ResultTIO[(Option[SquashStats], Key, Boolean)] = {
    if (dictionary.hasVirtual) {
      for {
        key    <- Repository.tmpDir(repository)
        hr     <- downcast[Repository, HdfsRepository](repository, s"Squash only works with Hdfs repositories currently, got '$repository'")
        inPath =  hr.toIvoryLocation(input).toHdfsPath
        ns     =  dictionary.byFeatureId.groupBy(_._1.namespace).keys.toList
        // This is about the best we can do at the moment, until we have more size information about each feature
        rs     <- ReducerSize.calculate(inPath, 1.gb).run(hr.configuration)
        prof   <- run(hr.configuration, rs, dictionary.byConcrete, date, inPath, hr.toIvoryLocation(key).toHdfsPath,
          hr.codec, conf)
      } yield (some(prof), key, true)
    } else
    // No virtual features, let's skip the entire squash MR
      (none[SquashStats], input, false).point[ResultTIO]
  }

  def run(conf: Configuration, reducers: Int, dictionary: DictionaryConcrete, date: Date, input: Path, output: Path,
          codec: Option[CompressionCodec], squashConf: SquashConfig): ResultTIO[SquashStats] = {

    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("ivory-squash", job)

    job.setJarByClass(classOf[SquashReducer])
    job.setJobName(ctx.id.value)

    // map
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    // partition & sort
    job.setPartitionerClass(classOf[SquashPartitioner])
    job.setGroupingComparatorClass(classOf[SquashWritable.GroupingByFeatureId])
    job.setSortComparatorClass(classOf[SquashWritable.ComparatorFeatureId])

    // reducer
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[SquashReducer])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    // input
    println(s"Input path: $input")
    MultipleInputs.addInputPath(job, input, classOf[SequenceFileInputFormat[_, _]], classOf[SquashMapper])

    // output
    val tmpout = new Path(ctx.output, "squash")
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, tmpout)

    // compression
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    // cache / config initialization
    job.getConfiguration.set(SnapshotJob.Keys.SnapshotDate, date.int.toString)
    job.getConfiguration.setInt(Keys.ProfileMod, squashConf.profileSampleRate)
    val (featureIdLookup, reductionLookup) = dictToLookup(dictionary, date)
    ctx.thriftCache.push(job, SnapshotJob.Keys.FeatureIdLookup, featureIdLookup)
    ctx.thriftCache.push(job, ReducerLookups.Keys.ReducerLookup,
      SquashReducerLookup.create(dictionary, featureIdLookup, reducers))
    ctx.thriftCache.push(job, Keys.ExpressionLookup, reductionLookup)

    // run job
    if (!job.waitForCompletion(true))
      Crash.error(Crash.ResultTIO, "ivory squash failed.")

    // commit files to factset
    Committer.commit(ctx, {
      case "squash" => output
    }, true).run(conf).as {
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

  def dictToLookup(dictionary: DictionaryConcrete, date: Date): (FeatureIdLookup, FeatureReductionLookup) = {
    val featureIdLookup = new FeatureIdLookup
    val reductionLookup = new FeatureReductionLookup
    dictionary.sources.zipWithIndex.foreach { case ((fid, cg), i) =>
      featureIdLookup.putToIds(fid.toString, i)
      reductionLookup.putToReductions(i, concreteGroupToReductions(date, fid, cg).asJava)
    }
    (featureIdLookup, reductionLookup)
  }

  def concreteGroupToReductions(date: Date, fid: FeatureId, cg: ConcreteGroup): List[FeatureReduction] = {
    // We use 'latest' reduction to output the concrete feature as well
    val cr = reductionToThriftExp(date, fid, Query(Latest, None), cg.definition.encoding, None)
    val vrs = cg.virtual.map((reductionToThrift(date, cg.definition.encoding) _).tupled)
    cr :: vrs
  }

  def reductionToThrift(date: Date, encoding: Encoding)(fid: FeatureId, vd: VirtualDefinition): FeatureReduction =
    reductionToThriftExp(date, fid, vd.query, encoding, vd.window)

  def reductionToThriftExp(date: Date, fid: FeatureId, query: Query, encoding: Encoding, window: Option[Window]): FeatureReduction = {
    val fr = new FeatureReduction(fid.namespace.name, fid.toString, fid.name, Expression.asString(query.expression), Encoding.render(encoding))
    query.filter.map(_.render).foreach(fr.setFilter)
    fr.setDate((query.expression match {
      // For latest and days since reducers, we need to match all facts (to catch them before the window).
      case Latest                         => Date.minValue
      // Days since is similar to latest, except an additional date operation is applied
      case BasicExpression(DaysSince)     => Date.minValue
      case StructExpression(_, DaysSince) => Date.minValue
      // If no window is specified the only functions we should be applying will deal with a single value,
      // and should _always_ apply; hence the min date
      case _      => window.cata(window => SnapshotWindows.startingDate(window, date), Date.minValue)
    }).int)
    fr
  }

  object Keys {
    val ProfileMod = "squash-profile-mod"
    val CounterTotalGroup = "squash-counter-total"
    val CounterSaveGroup = "squash-counter-save"
    val ProfileTotalGroup = "squash-profile-total"
    val ProfileSaveGroup = "squash-profile-save"
    val ExpressionLookup = ThriftCache.Key("squash-expression-lookup")
  }
}
