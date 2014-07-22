package com.ambiata.ivory.ingest

import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.{ReducerLookup, NamespaceLookup, FeatureIdLookup}
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.parse._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.mr._

import java.lang.{Iterable => JIterable}

import com.ambiata.ivory.storage.task.{FactsetJob, FactsReducer, FactsPartitioner}

import scalaz.{Reducer => _, _}, Scalaz._

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.TSerializer

import org.joda.time.DateTimeZone

/*
 * This is a hand-coded MR job to squeeze the most out of ingestion performance.
 */
object IngestJob {
  // FIX shouldn't need `root: Path` it is a workaround for poor namespace handling
  def run(conf: Configuration, dictionary: Dictionary, reducerLookups: ReducerLookups, ivoryZone: DateTimeZone, ingestZone: DateTimeZone, root: Path, paths: List[String], target: Path, errors: Path, codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = FactsetJob.configureJob("ivory-ingest", job, dictionary, reducerLookups, paths.map(new Path(_)), target, codec)

    /* map */
    job.setMapperClass(classOf[IngestMapper])

    /* input */
    job.setInputFormatClass(classOf[TextInputFormat])

    /* output */
    MultipleOutputs.addNamedOutput(job, Keys.Err,  classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable])

    /* cache / config initializtion */
    job.getConfiguration.set(Keys.IvoryZone, ivoryZone.getID)
    job.getConfiguration.set(Keys.IngestZone, ingestZone.getID)
    job.getConfiguration.set(Keys.IngestBase, FileSystem.get(conf).getFileStatus(root).getPath.toString)

    /* run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory ingest failed.")

    /* commit files to factset */
    Committer.commit(ctx, {
      case "errors"  => errors
      case "factset" => target
    }, true).run(conf).run.unsafePerformIO
  }

  object Keys {
    val IvoryZone = "ivory.tz"
    val IngestZone = "ivory.ingest.tz"
    val IngestBase = "ivory.ingest.base"
    val Err = "err"
  }
}

/*
 * Mapper for ivory-ingest.
 *
 * The input is a standard TextInputFormat.
 *
 * The output key is a long, where the top 32 bits is an externalized feature id and
 * the bottom 32 bits is an ivory date representation of the yyyy-MM-dd of the fact.
 *
 * The output value is the already serialized bytes of the fact ready to write.
 */
class IngestMapper extends Mapper[LongWritable, Text, LongWritable, BytesWritable] {
  /* Context object contains tmp paths and dist cache */
  var ctx: MrContext = null

  /* Cache for path -> namespace mapping. */
  val namespaces: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty

  /* Value serializer. */
  val serializer = new TSerializer(new TCompactProtocol.Factory)

  /* Error output channel, see #setup */
  var out: MultipleOutputs[NullWritable, BytesWritable] = null

  /* FeatureId.toString -> Int mapping for externalizing feature id, see #setup */
  val lookup = new FeatureIdLookup

  /* Dictionary for this load, see #setup. */
  var dict: Dictionary = null

  /* Ivory repository time zone, see #setup. */
  var ivoryZone: DateTimeZone = null

  /* Ingestion time zone, see #setup. */
  var ingestZone: DateTimeZone = null

  /* Base path for this load, used to determine namespace, see #setup.
     FIX: this is hacky as (even more than the rest) we should rely on
     a marker file or something more sensible that a directory name. */
  var base: String = null

  /* The output key, only create once per mapper. */
  val kout = new LongWritable

  /* The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  /* Path this mapper is processing */
  var splitPath: Path = null

  override def setup(context: Mapper[LongWritable, Text, LongWritable, BytesWritable]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    out = new MultipleOutputs(context.asInstanceOf[Mapper[LongWritable, Text, NullWritable, BytesWritable]#Context])
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.FeatureIdLookup, lookup)
    val dictThrift = new ThriftDictionary
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.Dictionary, dictThrift)
    dict = DictionaryThriftConversion.dictionary.from(dictThrift)
    ivoryZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IvoryZone))
    ingestZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IngestZone))
    base = context.getConfiguration.get(IngestJob.Keys.IngestBase)
    splitPath = MrContext.getSplitPath(context.getInputSplit)
  }

  override def cleanup(context: Mapper[LongWritable, Text, LongWritable, BytesWritable]#Context): Unit =
    out.close()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, LongWritable, BytesWritable]#Context): Unit = {
    val line = value.toString

    EavtParsers.parse(line, dict, namespaces.getOrElseUpdate(splitPath.getParent.toString, findIt(splitPath)), ingestZone) match {
      case Success(f) =>

        context.getCounter("ivory", "ingest.ok").increment(1)

        val k = lookup.ids.get(f.featureId.toString).toInt
        kout.set((k.toLong << 32) | f.date.int.toLong)

        val v = serializer.serialize(f.toThrift)
        vout.set(v, 0, v.length)

        context.write(kout, vout)

      case Failure(e) =>

        context.getCounter("ivory", "ingest.error").increment(1)

        val v = serializer.serialize(new ThriftParseError(line, e))
        vout.set(v, 0, v.length)

        out.write(IngestJob.Keys.Err, NullWritable.get, vout, "errors/part")
    }
  }

  def findIt(p: Path): String =
    if (p.getParent.toString == base)
      p.getName
    else
      findIt(p.getParent)
}
