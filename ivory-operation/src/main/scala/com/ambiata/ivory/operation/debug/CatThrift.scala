package com.ambiata.ivory.operation.debug

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.MrContextIvory
import com.ambiata.ivory.operation.ingestion.thrift._
import com.ambiata.mundane.control._
import com.ambiata.poacher.mr._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import scala.collection.JavaConverters._

/** This will eventually move to a separate ivory-tools repository */
object CatThrift {

  def run(configuration: Configuration, entities: List[String], input: String, format: CatThriftFormat, output: String,
          codec: Option[CompressionCodec]): RIO[Unit] =
    job(configuration, entities, new Path(input), format, new Path(output), codec)

  def job(configuration: Configuration, entities: List[String], input: Path, format: CatThriftFormat, output: Path,
          codec: Option[CompressionCodec]): RIO[Unit] = for {
    job <- RIO.io { Job.getInstance(configuration) }
    ctx <- RIO.io { MrContextIvory.newContext("ivory-cat-thrift", job) }
    r   <- RIO.io {
        job.setJarByClass(classOf[CatThriftDenseMapper])
        job.setJobName(ctx.id.value)
        job.setMapOutputKeyClass(classOf[NullWritable])
        job.setMapOutputValueClass(classOf[Text])
        job.setNumReduceTasks(0)
        MultipleInputs.addInputPath(job, input, classOf[SequenceFileInputFormat[_, _]], format match {
          case CatThriftFact   => classOf[CatThriftFactMapper]
          case CatThriftDense  => classOf[CatThriftDenseMapper]
          case CatThriftSparse => classOf[CatThriftSparseMapper]
        })
        val tmpout = new Path(ctx.output, "out")
        job.setOutputFormatClass(classOf[TextOutputFormat[_, _]])
        FileOutputFormat.setOutputPath(job, tmpout)
        codec.foreach(cc => {
          Compress.intermediate(job, cc)
          Compress.output(job, cc)
        })
        DumpFactsJob.write(job.getConfiguration, Keys.Entities, entities)

        job.waitForCompletion(true)
      }
    _   <- RIO.unless(r, RIO.fail("Ivory dump facts failed to complete, please see job tracker."))
    _   <- Committer.commit(ctx, {
          case "out" => output
        }, true).run(configuration)
  } yield ()

  object Keys {
    val Entities = "ivory.cat-thrift.entities"
  }
}

sealed trait CatThriftFormat
case object CatThriftFact extends CatThriftFormat
case object CatThriftDense extends CatThriftFormat
case object CatThriftSparse extends CatThriftFormat

object CatThriftFormat {

  val formats: Map[String, CatThriftFormat] = Map(
    "fact" -> CatThriftFact,
    "dense" -> CatThriftDense,
    "sparse" -> CatThriftSparse
  )

  def parseFormat(input: String): Option[CatThriftFormat] =
    formats.get(input.toLowerCase)
}

abstract class CatThriftMapper[A](implicit ev: A <:< ThriftLike) extends Mapper[NullWritable, BytesWritable, NullWritable, Text] {

  val serializer = ThriftSerialiser()
  val buffer = new StringBuilder(4096)
  val out = new Text
  var entities: Set[String] = null

  val delimiter = '|'
  val missingValue = "NA"

  val thrift: A

  override def setup(context: Mapper[NullWritable, BytesWritable, NullWritable, Text]#Context): Unit = {
    entities = DumpFactsJob.read(context.getConfiguration, DumpFactsJob.Keys.Entities).toSet
  }

  override def map(key: NullWritable, value: BytesWritable, context: Mapper[NullWritable, BytesWritable, NullWritable, Text]#Context): Unit = {
    serializer.fromBytesViewUnsafe(thrift, value.getBytes, 0, value.getLength)
    if (entities.isEmpty || entities.contains(getEntity(thrift))) {
      buffer.setLength(0)
      renderWith(thrift, buffer)
      if (buffer.nonEmpty) {
        out.set(buffer.toString())
        context.write(key, out)
      }
    }
  }

  def valueToString(thriftValue: ThriftFactValue): String =
    // This isn't ideal - it's converting to our internal thrift format first, and then to Value and _then_ to String
    Conversion.thrift2value(thriftValue).fold(
      identity,
      value => Value.json.toStringWithStruct(Value.fromThrift(value), missingValue)
    )

  def getEntity(thrift: A): String
  def renderWith(thrift: A, buffer: StringBuilder): Unit
}

class CatThriftDenseMapper extends CatThriftMapper[ThriftFactDense] {
  val thrift = new ThriftFactDense

  def getEntity(thrift: ThriftFactDense): String =
    thrift.entity

  def renderWith(thrift: ThriftFactDense, buffer: StringBuilder): Unit = {
    buffer.append(thrift.entity)
    val values = thrift.getValue
    var i = 0
    while (i < values.size()) {
      buffer.append(delimiter)
      buffer.append(valueToString(values.get(i)))
      i += 1
    }
  }
}

class CatThriftSparseMapper extends CatThriftMapper[ThriftFactSparse] {
  val thrift = new ThriftFactSparse

  def getEntity(thrift: ThriftFactSparse): String =
    thrift.entity

  def renderWith(thrift: ThriftFactSparse, buffer: StringBuilder): Unit = {
    thrift.getValue.asScala.foreach { case (feature, value) =>
      buffer.append(thrift.entity)
      buffer.append(delimiter)
      buffer.append(feature)
      buffer.append(delimiter)
      buffer.append(valueToString(value))
      buffer.append("\n")
    }
    // Remove the last newline
    if (buffer.nonEmpty) {
      buffer.setLength(buffer.length - 1)
    }
  }
}

class CatThriftFactMapper extends CatThriftMapper[ThriftFact] {
  val thrift = new ThriftFact

  def getEntity(thrift: ThriftFact): String =
    thrift.entity

  def renderWith(thrift: ThriftFact, buffer: StringBuilder): Unit = {
    buffer.append(thrift.entity)
    buffer.append(delimiter)
    buffer.append(thrift.attribute)
    buffer.append(delimiter)
    buffer.append(valueToString(thrift.getValue))
    buffer.append(delimiter)
    buffer.append(thrift.datetime)
    ()
  }
}
