package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.MrContextIvory
import com.ambiata.poacher.mr._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce.{Counter => _, _}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/**
 * This is a hand-coded MR job to squeeze the most out of sparse performance.
 */
object SparseOutputJob {
  def run(conf: Configuration, dictionary: Dictionary, input: Path, output: Path, missing: String,
          delimiter: Char, escaped: Boolean, codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = MrContextIvory.newContext("ivory-sparse", job)

    job.setJarByClass(classOf[SparseOutputMapper])
    job.setJobName(ctx.id.value)

    // input
    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])
    FileInputFormat.addInputPaths(job, input.toString)

    // map
    job.setMapperClass(classOf[SparseOutputMapper])
    job.setMapOutputKeyClass(classOf[NullWritable])
    job.setMapOutputValueClass(classOf[Text])

    // reduce
    job.setNumReduceTasks(0)

    // output
    val tmpout = new Path(ctx.output, "eav")
    job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
    FileOutputFormat.setOutputPath(job, tmpout)

    // compression
    codec.foreach(cc => {
      Compress.intermediate(job, cc)
      Compress.output(job, cc)
    })

    // cache / config initializtion
    job.getConfiguration.set(Keys.Missing, missing)
    job.getConfiguration.set(Keys.Delimiter, delimiter.toString)
    TextEscaper.toConfiguration(job.getConfiguration, escaped)

    // run job
    if (!job.waitForCompletion(true))
      sys.error("ivory eav failed.")

    Committer.commit(ctx, {
      case "eav" => output
    }, true).run(conf).run.unsafePerformIO()

    DictionaryOutput.writeToHdfs(output, dictionary, Some(missing), delimiter).run(conf).run.unsafePerformIO()
    ()
  }

  object Keys {
    val Missing = "ivory.eav.missing"
    val Delimiter = "ivory.eav.delimiter"
  }
}

/**
 * Mapper for ivory-eav.
 *
 * The input is a standard SequenceFileInputFormat. Where the key is empty and the value
 * is thrift encoded bytes of a NamespacedThriftFact.
 *
 * The output is EAV text
 */
class SparseOutputMapper extends Mapper[NullWritable, BytesWritable, NullWritable, Text] {
  /** Thrift deserializer. */
  val serializer = ThriftSerialiser()

  /** Empty Fact, created once per reducer and mutated per record */
  val fact = createMutableFact

  /** Output key, only create once per reducer */
  val kout = NullWritable.get

  /** Output value, only create once per reducer */
  val vout = new Text

  /** missing value to use in place of empty values. */
  var missing: String = null

  /** delimiter value to use in output. */
  var delimiter = '?'

  /** Running output buffer for a row. */
  val buffer = new StringBuilder(4096)

  var escapeAppend: TextEscaper.Append = null

  override def setup(context: Mapper[NullWritable, BytesWritable, NullWritable, Text]#Context): Unit = {
    missing = context.getConfiguration.get(SparseOutputJob.Keys.Missing)
    delimiter = context.getConfiguration.get(SparseOutputJob.Keys.Delimiter).charAt(0)
    escapeAppend = TextEscaper.fromConfiguration(context.getConfiguration, delimiter)
  }

  /** Read and pass through, extracting entity and feature id for sort phase. */
  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, NullWritable, Text]#Context): Unit = {
    serializer.fromBytesViewUnsafe(fact, value.getBytes, 0, value.getLength)

    buffer.setLength(0)
    buffer.append(fact.entity)
    buffer.append(delimiter)
    buffer.append(fact.namespace.name)
    buffer.append(delimiter)
    buffer.append(fact.feature)
    buffer.append(delimiter)
    escapeAppend(buffer, Value.toStringWithStruct(fact.value, missing))

    vout.set(buffer.toString())
    context.write(kout, vout)
  }
}
