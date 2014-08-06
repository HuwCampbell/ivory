package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup.FeatureIdLookup
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.parse._
import com.ambiata.ivory.mr._

import com.ambiata.ivory.storage.task.FactsetJob

import scalaz.{Reducer => _, _}, Scalaz._

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{MultipleOutputs, SequenceFileOutputFormat}
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TException, TSerializer}

import org.joda.time.DateTimeZone

/*
 * This is a hand-coded MR job to squeeze the most out of ingestion performance.
 */
object IngestJob {
  // FIX shouldn't need `root: Path` it is a workaround for poor namespace handling
  def run(conf: Configuration, dictionary: Dictionary, reducerLookups: ReducerLookups, ivoryZone: DateTimeZone,
          ingestZone: DateTimeZone, root: Path, singleNamespace: Option[String], paths: List[Path], target: Path,
          errors: Path, format: Format, codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = FactsetJob.configureJob("ivory-ingest", job, dictionary, reducerLookups, paths, target, codec)

    /* map */
    job.setMapperClass(format match {
      case TextFormat   => classOf[TextIngestMapper]
      case ThriftFormat => classOf[ThriftIngestMapper]
    })

    /* input */
    job.setInputFormatClass(format match {
      case TextFormat   => classOf[TextInputFormat]
      case ThriftFormat => classOf[SequenceFileInputFormat[_, _]]
    })

    /* output */
    MultipleOutputs.addNamedOutput(job, Keys.Err,  classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable])

    /* cache / config initializtion */
    job.getConfiguration.set(Keys.IvoryZone, ivoryZone.getID)
    job.getConfiguration.set(Keys.IngestZone, ingestZone.getID)
    job.getConfiguration.set(Keys.IngestBase, FileSystem.get(conf).getFileStatus(root).getPath.toString)
    singleNamespace.foreach(ns => job.getConfiguration.set(Keys.SingleNamespace, ns))

    /* run job */
    if (!job.waitForCompletion(true))
      sys.error("ivory ingest failed.")

    /* commit files to factset */
    Committer.commit(ctx, {
      case "errors"  => errors
      case "factset" => target
    }, true).run(conf).run.unsafePerformIO()
  }

  object Keys {
    val IvoryZone = "ivory.tz"
    val IngestZone = "ivory.ingest.tz"
    val IngestBase = "ivory.ingest.base"
    val SingleNamespace = "ivory.ingest.singlenamespace"
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
trait IngestMapper[K, I] extends Mapper[K, I, LongWritable, BytesWritable] {
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

  /* name of the namespace being processed if there's only one */
  var singleNamespace: Option[String] = None

  override def setup(context: Mapper[K, I, LongWritable, BytesWritable]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    out = new MultipleOutputs(context.asInstanceOf[Mapper[LongWritable, I, NullWritable, BytesWritable]#Context])
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.FeatureIdLookup, lookup)
    val dictThrift = new ThriftDictionary
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.Dictionary, dictThrift)
    dict = DictionaryThriftConversion.dictionary.from(dictThrift)
    ivoryZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IvoryZone))
    ingestZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IngestZone))
    base = context.getConfiguration.get(IngestJob.Keys.IngestBase)
    splitPath = MrContext.getSplitPath(context.getInputSplit)
    singleNamespace = Option(context.getConfiguration.get(IngestJob.Keys.SingleNamespace))
  }

  override def cleanup(context: Mapper[K, I, LongWritable, BytesWritable]#Context): Unit =
    out.close()

  override def map(key: K, value: I, context: Mapper[K, I, LongWritable, BytesWritable]#Context): Unit = {
    val namespace = singleNamespace.fold(namespaces.getOrElseUpdate(splitPath.getParent.toString, findIt(splitPath)))(identity)

    parse(namespace, value) match {
      case Success(f) =>

        context.getCounter("ivory", "ingest.ok").increment(1)

        val k = lookup.ids.get(f.featureId.toString).toInt
        kout.set((k.toLong << 32) | f.date.int.toLong)

        val v = serializer.serialize(f.toThrift)
        vout.set(v, 0, v.length)

        context.write(kout, vout)

      case Failure(e) =>

        context.getCounter("ivory", "ingest.error").increment(1)

        val v = serializer.serialize(e.toThrift)
        vout.set(v, 0, v.length)

        out.write(IngestJob.Keys.Err, NullWritable.get, vout, "errors/part")
    }
  }

  def parse(namespace: String, v: I): Validation[ParseError, Fact]

  def findIt(p: Path): String =
    if (p.getParent.toString == base)
      p.getName
    else
      findIt(p.getParent)
}

class TextIngestMapper extends IngestMapper[LongWritable, Text] {

  override def parse(namespace: String, value: Text): Validation[ParseError, Fact] = {
    val line = value.toString
    EavtParsers.parse(line, dict, namespace, ingestZone).leftMap(ParseError(_, TextError(line)))
  }
}

class ThriftIngestMapper extends IngestMapper[NullWritable, BytesWritable] {

  import com.ambiata.ivory.operation.ingestion.thrift._
  import scodec.bits.ByteVector

  val deserializer = ThriftSerialiser()
  val thrift = new ThriftFact

  override def parse(namespace: String, value: BytesWritable): Validation[ParseError, Fact] = {
    thrift.clear()
    ((try deserializer.fromBytesViewUnsafe(thrift, value.getBytes, 0, value.getLength).right catch {
      case e: TException => e.left
    }) match {
      case -\/(e)  => e.getMessage.failure
      case \/-(_)  => Conversion.thrift2fact(namespace, thrift, ingestZone, ivoryZone).validation
    // TODO Use ByteView.view() when it has length
    }).leftMap(ParseError(_, ThriftError(ThriftErrorDataVersionV1, ByteVector(value.getBytes))))
  }
}
