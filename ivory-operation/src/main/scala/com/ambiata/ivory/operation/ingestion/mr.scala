package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.FeatureIdLookup
import com.ambiata.ivory.mr.{DictionaryCache, MapString}
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.ivory.storage.parse._
import com.ambiata.ivory.storage.task.{FactsetWritable, FactsetJob}
import com.ambiata.mundane.control._
import com.ambiata.poacher.mr._

import scalaz.{Name =>_, Reducer => _, Value => _, _}, Scalaz._

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{MultipleOutputs, SequenceFileOutputFormat}
import org.apache.thrift.TException

import org.joda.time.DateTimeZone

/**
 * This is a hand-coded MR job to squeeze the most out of ingestion performance.
 */
object IngestJob {
  def run(conf: Configuration, dictionary: Dictionary, reducerLookups: ReducerLookups, ivoryZone: DateTimeZone,
          ingestZone: Option[DateTimeZone], inputs: List[(FileFormat, Option[Namespace], Path, List[Path])], target: Path,
          errors: Path, codec: Option[CompressionCodec]): RIO[Unit] = {

    val job = Job.getInstance(conf)
    val ctx = FactsetJob.configureJob("ivory-ingest", job, dictionary, reducerLookups, target, codec)

    inputs.foreach { case (format, _, _, paths) =>
      val mc = format.fold({ case (_, escaping) => escaping match {
        case TextEscaping.Delimited => classOf[TextDelimitedIngestMapper]
        case TextEscaping.Escaped   => classOf[TextEscapedIngestMapper]
      }}, classOf[ThriftIngestMapper])
      val ifc =
        if (format.isText) classOf[TextInputFormat]
        else classOf[SequenceFileInputFormat[_, _]]
      paths.foreach(MultipleInputs.addInputPath(job, _, ifc, mc))
    }

    // output
    MultipleOutputs.addNamedOutput(job, Keys.Err,  classOf[SequenceFileOutputFormat[_, _]],  classOf[NullWritable], classOf[BytesWritable])

    // cache / config initialization
    job.getConfiguration.set(Keys.IvoryZone, ivoryZone.getID)
    // At the last minute we use the same zone for ingest, Joda will (nicely) not do any extra conversion in this case
    job.getConfiguration.set(Keys.IngestZone, ingestZone.getOrElse(ivoryZone).getID)
    // Write out the specified namespaces, the rest will use the split information
    job.getConfiguration.set(Keys.Namespaces, MapString.render(inputs.flatMap {
      case (_, ns, root, _) => ns.map(n => FileSystem.get(conf).getFileStatus(root).getPath.toString -> n.name)
    }.toMap))
    job.getConfiguration.set(Keys.Delims, MapString.render(inputs.flatMap {
      case (format, ns, root, _) => format.fold({ case (delim, _) => Some(root -> delim.character) },  None)
    }.map(x => FileSystem.get(conf).getFileStatus(x._1).getPath.toString -> x._2.toString).toMap))

    // run job
    if (!job.waitForCompletion(true))
      Crash.error(Crash.RIO, "ivory ingest failed.")

    // commit files to factset
    Committer.commit(ctx, {
      case "errors"  => errors
      case "factset" => target
    }, true).run(conf)
  }

  object Keys {
    val IvoryZone = "ivory.tz"
    val IngestZone = "ivory.ingest.tz"
    val Namespaces = "ivory.ingest.namespaces"
    val Delims = "ivory.ingest.delims"
    val Err = "err"
  }
}

/**
 * Mapper for ivory-ingest.
 *
 * The input is a standard TextInputFormat.
 *
 * The output key is a long, where the top 32 bits is an externalized feature id and
 * the bottom 32 bits is an ivory date representation of the yyyy-MM-dd of the fact.
 *
 * The output value is the already serialized bytes of the fact ready to write.
 */
trait IngestMapper[K, I] extends Mapper[K, I, BytesWritable, BytesWritable] {
  /** Context object contains tmp paths and dist cache */
  var ctx: MrContext = null

  /** Value serializer. */
  val serializer = ThriftSerialiser()

  /** Error output channel, see #setup */
  var out: MultipleOutputs[NullWritable, BytesWritable] = null

  /** FeatureId.toString -> Int mapping for externalizing feature id, see #setup */
  val lookup = new FeatureIdLookup

  /** Dictionary for this load, see #setup. */
  var dict: Dictionary = null

  /** Ivory repository time zone, see #setup. */
  var ivoryZone: DateTimeZone = null

  /** Ingestion time zone, see #setup. */
  var ingestZone: DateTimeZone = null

  /** Path to specific namespace, otherwise we fall back to the parent directory name */
  var bases: Map[String, String] = null

  /** The output key, only create once per mapper. */
  val kout = FactsetWritable.create

  /** The output value, only create once per mapper. */
  val vout = Writables.bytesWritable(4096)

  var namespace: String = null

  override def setup(context: Mapper[K, I, BytesWritable, BytesWritable]#Context): Unit = {
    ctx = MrContext.fromConfiguration(context.getConfiguration)
    out = new MultipleOutputs(context.asInstanceOf[Mapper[K, I, NullWritable, BytesWritable]#Context])
    ctx.thriftCache.pop(context.getConfiguration, ReducerLookups.Keys.FeatureIdLookup, lookup)
    dict = DictionaryCache.load(context.getConfiguration, ctx.thriftCache)
    ivoryZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IvoryZone))
    ingestZone = DateTimeZone.forID(context.getConfiguration.get(IngestJob.Keys.IngestZone))
    bases = MapString.fromString(context.getConfiguration.get(IngestJob.Keys.Namespaces, ""))
    val splitPath = MrContext.getSplitPath(context.getInputSplit)
    namespace = bases.getOrElse(splitPath.getParent.toString, splitPath.getParent.getName)
  }

  override def cleanup(context: Mapper[K, I, BytesWritable, BytesWritable]#Context): Unit =
    out.close()

  override def map(key: K, value: I, context: Mapper[K, I, BytesWritable, BytesWritable]#Context): Unit = {

    parse(Namespace.unsafe(namespace), value) match {

      case Success(f) =>

        context.getCounter("ivory", "ingest.ok").increment(1)

        val k = lookup.ids.get(f.featureId.toString).toInt
        FactsetWritable.set(f, kout, k)

        val v = serializer.toBytes(f.toThrift)
        vout.set(v, 0, v.length)

        context.write(kout, vout)

      case Failure(e) =>

        context.getCounter("ivory", "ingest.error").increment(1)

        val v = serializer.toBytes(e.toThrift)
        vout.set(v, 0, v.length)

        out.write(IngestJob.Keys.Err, NullWritable.get, vout, "errors/part")
    }
  }

  def parse(namespace: Namespace, v: I): Validation[ParseError, Fact]
}

trait TextIngestMapper extends IngestMapper[LongWritable, Text] {

  var delim: Char = '|'
  val splitter: String => List[String]

  override def setup(context: Mapper[LongWritable, Text, BytesWritable, BytesWritable]#Context): Unit = {
    super.setup(context)
    val splitPath = MrContext.getSplitPath(context.getInputSplit)
    delim = MapString.fromString(context.getConfiguration.get(IngestJob.Keys.Delims, "")).getOrElse(splitPath.toString, "|").charAt(0)
  }

  override def parse(namespace: Namespace, value: Text): Validation[ParseError, Fact] = {
    val line = value.toString
    EavtParsers.parser(dict, namespace, ivoryZone, ingestZone).run(splitter(line)).leftMap(ParseError(_, TextError(line)))
  }
}

class TextDelimitedIngestMapper extends TextIngestMapper {

  val splitter = (line: String) => EavtParsers.splitLine(delim, line)
}

class TextEscapedIngestMapper extends TextIngestMapper {

  val splitter = (line: String) => TextEscaping.split(delim, line)
}

class ThriftIngestMapper extends IngestMapper[NullWritable, BytesWritable] {

  import com.ambiata.ivory.operation.ingestion.thrift._
  import scodec.bits.ByteVector

  val deserializer = ThriftSerialiser()
  val thrift = new ThriftFact

  override def parse(namespace: Namespace, value: BytesWritable): Validation[ParseError, Fact] = {
    thrift.clear()
    try deserializer.fromBytesViewUnsafe(thrift, value.getBytes, 0, value.getLength)
    catch {
      case e: TException =>
        Crash.error(Crash.DataIntegrity, s"Thrift could not be deserialised: '${e.getMessage}' in namepsace ${namespace.name}")
    }
    if (thrift.value == null) {
      Crash.error(Crash.DataIntegrity, s"Invalid Thrift data: for namespace ${namespace.name} with entity ${thrift.entity}")
    }
    Conversion.thrift2fact(namespace.name, thrift, ingestZone, ivoryZone).fold(
      e => e.failure,
      Fact.validate(_, dict)
    ).leftMap(ParseError(_, ThriftError(ThriftErrorDataVersionV1, ByteVector.view(value.getBytes).take(value.getLength))))
  }
}
