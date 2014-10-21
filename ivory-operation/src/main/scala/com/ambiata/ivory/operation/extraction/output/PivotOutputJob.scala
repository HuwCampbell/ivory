package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.lookup._
import com.ambiata.ivory.storage.lookup._
import com.ambiata.ivory.mr._

import java.lang.{Iterable => JIterable}

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
 * This is a hand-coded MR job to squeeze the most out of pivot performance.
 */
object PivotOutputJob {
  def run(conf: Configuration, dictionary: Dictionary, input: Path, output: Path, missing: String,
          delimiter: Char, reducers: Int, codec: Option[CompressionCodec]): Unit = {

    val job = Job.getInstance(conf)
    val ctx = MrContext.newContext("ivory-pivot", job)

    job.setJarByClass(classOf[PivotMapper])
    job.setJobName(ctx.id.value)

    // input
    job.setInputFormatClass(classOf[SequenceFileInputFormat[_, _]])
    FileInputFormat.addInputPaths(job, input.toString)

    // map
    job.setMapperClass(classOf[PivotMapper])
    job.setMapOutputKeyClass(classOf[BytesWritable])
    job.setMapOutputValueClass(classOf[BytesWritable])

    // partition & sort
    job.setPartitionerClass(classOf[PivotPartitioner])
    job.setGroupingComparatorClass(classOf[PivotGrouping])
    job.setSortComparatorClass(classOf[BytesWritable.Comparator])

    // reducer
    job.setNumReduceTasks(reducers)
    job.setReducerClass(classOf[PivotReducer])

    // output
    val tmpout = new Path(ctx.output, "pivot")
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
    ctx.thriftCache.push(job, Keys.Dictionary, DictionaryThriftConversion.dictionaryToThrift(dictionary))
    val (_, lookup) = ReducerLookups.indexDefinitions(dictionary.sortedByFeatureId)
    ctx.thriftCache.push(job, Keys.FeatureIds, lookup)

    // run job
    if (!job.waitForCompletion(true))
      sys.error("ivory pivot failed.")

    Committer.commit(ctx, {
      case "pivot" => output
    }, true).run(conf).run.unsafePerformIO()

    DictionaryOutput.writeToHdfs(output, dictionary, missing, delimiter).run(conf).run.unsafePerformIO()
    ()
  }

  object Keys {
    val Missing = "ivory.pivot.missing"
    val Delimiter = "ivory.pivot.delimiter"
    val Dictionary = ThriftCache.Key("ivory.pivot.lookup.dictionary")
    val FeatureIds = ThriftCache.Key("ivory.pivot.lookup.featureid")
  }
}

/**
 * Mapper for ivory-pivot.
 *
 * The input is a standard SequenceFileInputFormat. Where the key is empty and the value
 * is thrift encoded bytes of a NamespacedThriftFact.
 *
 * The output key is the entity id and namespace.
 * The output value is the same as the input value.
 */
class PivotMapper extends Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable] {
  /** Thrift deserializer. */
  val serializer = ThriftSerialiser()

  /** Empty Fact, created once per reducer and mutated per record */
  val fact = createMutableFact

  /** Output key container */
  val kout = new BytesWritable

  /** Indexed view of dictionary for this run, see #setup. */
  val features = new FeatureIdLookup

  override def setup(context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, PivotOutputJob.Keys.FeatureIds, features)
  }

  /** Read and pass through, extracting entity and feature id for sort phase. */
  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, BytesWritable, BytesWritable]#Context): Unit = {
    serializer.fromBytesViewUnsafe(fact, value.getBytes, 0, value.getLength)
    val entity = fact.entity.getBytes("UTF-8")
    val required = entity.length + 4
    if (kout.getCapacity < required)
      kout.setCapacity(required * 2)
    kout.setSize(required)
    val bytes = kout.getBytes
    System.arraycopy(entity, 0, bytes, 0, entity.length)
    ByteWriter.writeInt(bytes, features.getIds.get(fact.featureId.toString), entity.length)
    context.write(kout, value)
  }
}

/**
 * Reducer for ivory-pivot.
 *
 * This reducer takes the latest namespaced fact, in entity|namespace|attribute order.
 *
 * The input values are serialized containers of namespaced thrift facts.
 *
 * The output is a PSV text file.
 */
class PivotReducer extends Reducer[BytesWritable, BytesWritable, NullWritable, Text] {
  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Empty Fact, created once per reducer and mutated per record */
  val fact = new NamespacedThriftFact with NamespacedThriftFactDerived

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

  var features: Array[String] = null

  override def setup(context: Reducer[BytesWritable, BytesWritable, NullWritable, Text]#Context): Unit = {
    import scala.collection.JavaConverters._
    val lookup = new FeatureIdLookup
    val ctx = MrContext.fromConfiguration(context.getConfiguration)
    ctx.thriftCache.pop(context.getConfiguration, PivotOutputJob.Keys.FeatureIds, lookup)
    features = lookup.getIds.asScala.toList.sortBy(_._2).map(_._1).toArray
    missing = context.getConfiguration.get(PivotOutputJob.Keys.Missing)
    delimiter = context.getConfiguration.get(PivotOutputJob.Keys.Delimiter).charAt(0)
  }

  override def reduce(key: BytesWritable, iterable: JIterable[BytesWritable], context: Reducer[BytesWritable, BytesWritable, NullWritable, Text]#Context): Unit = {
    buffer.setLength(0)
    var first = true
    val iter = iterable.iterator
    var i = 0; while (iter.hasNext) {
      val next = iter.next
      serializer.fromBytesViewUnsafe(fact, next.getBytes, 0, next.getLength)

      if (first) {
        buffer.append(fact.entity)
      }

      while (i < features.length && features(i) != fact.featureId.toString) {
        buffer.append(delimiter)
        buffer.append(missing)
        i += 1
      }

      buffer.append(delimiter)
      if (i <= features.length) {
        val s = Value.toStringOr(fact.value, missing).getOrElse(sys.error(s"Could not render fact ${fact.toString}"))
        buffer.append(s)
      }
      first = false
      i += 1
    }
    while (i < features.length) {
      buffer.append(delimiter)
      buffer.append(missing)
      i += 1
    }

    vout.set(buffer.toString())
    context.write(kout, vout)
  }
}

/** Group by just the entity and ignore featureId */
class PivotGrouping extends RawBytesComparator {
  def compareRaw(bytes1: Array[Byte], offset1: Int, length1: Int, bytes2: Array[Byte], offset2: Int, length2: Int): Int =
    compareBytes(bytes1, offset1, length1 - 4, bytes2, offset2, length2 - 4)
}

/** Partition by just the entity and ignore featureId */
class PivotPartitioner extends Partitioner[BytesWritable, BytesWritable] {
  override def getPartition(k: BytesWritable, v: BytesWritable, partitions: Int): Int =
    (WritableComparator.hashBytes(k.getBytes, 0, k.getLength - 4) & 0x7fffffff) % partitions
}
