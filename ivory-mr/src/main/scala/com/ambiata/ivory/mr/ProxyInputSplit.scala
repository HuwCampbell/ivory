package com.ambiata.ivory.mr

import com.ambiata.notion.distcopy.Partition

import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.serializer.SerializationFactory
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.ReflectionUtils

class ProxyInputSplit(
  var splits: Array[InputSplit],
  var formatClass: Class[_ <: InputFormat[_, _]],
  var mapperClass: Class[_ <: CombinableMapper[_, _, _, _]],
  var configuration: Configuration
) extends InputSplit with Configurable with Writable {

  def this() =
    this(null, null, null, null)

  override def getLength: Long = {
    var length = 0L
    splits.foreach(split => length += split.getLength)
    length
  }

  override def getLocations: Array[String] =
    splits.flatMap(split => split.getLocations).toSet.toArray

  override def readFields(in: DataInput): Unit = {
    val splitClass = readClass(in).asInstanceOf[Class[InputSplit]]
    formatClass = readClass(in).asInstanceOf[Class[_ <: InputFormat[_, _]]]
    mapperClass = readClass(in).asInstanceOf[Class[_ <: CombinableMapper[_, _, _, _]]]
    val factory = new SerializationFactory(configuration)
    val deserializer = factory.getDeserializer(splitClass)
    val length = in.readInt
    deserializer.open(in.asInstanceOf[DataInputStream])
    splits = (1 to length).toList.map(_ => {
      val empty = ReflectionUtils.newInstance(splitClass, configuration).asInstanceOf[InputSplit]
      val split = deserializer.deserialize(empty).asInstanceOf[InputSplit]
      split
    }).toArray
  }

  def readClass(in: DataInput): Class[_] =
    configuration.getClassByName(Text.readString(in))

  override def write(out: DataOutput): Unit = {
    val splitClass = splits(0).getClass.asInstanceOf[Class[InputSplit]]
    Text.writeString(out, splitClass.getName)
    Text.writeString(out, formatClass.getName)
    Text.writeString(out, mapperClass.getName)
    out.writeInt(splits.size)
    val factory = new SerializationFactory(configuration)
    val serializer = factory.getSerializer(splitClass)
    serializer.open(out.asInstanceOf[DataOutputStream])
    splits.foreach(split => serializer.serialize(split))
  }

  override def getConf: Configuration =
    configuration

  override def setConf(c: Configuration): Unit =
    configuration = c

  override def toString: String =
    s"Proxy[${splits.mkString(",")}]"
}
