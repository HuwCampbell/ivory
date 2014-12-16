package com.ambiata.ivory.mr

import com.ambiata.ivory.core._
import com.ambiata.poacher.mr._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.notion.distcopy.Partition

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.ReflectionUtils

import scala.collection.JavaConverters._

class ProxyInputFormat[A, B] extends InputFormat[SplitKey[A], B]  {
  def getLimit(configuration: Configuration): Long =
    configuration.getLong("dfs.blocksize",
      configuration.getLong("dfs.block.size",
        128.mb.toBytes.value))

  def getSplits(context: JobContext) : java.util.List[InputSplit] = {
    val configuration = context.getConfiguration
    val job = Job.getInstance(configuration)
    val ctx = MrContext.fromConfiguration(configuration)
    val inputs = ProxyInputFormat.take(ctx).getOrElse(Crash.error(Crash.Invariant, "ProxyInputFormat not configured, requires a call to ProxyInputFomat.configure first."))
    val out = new java.util.ArrayList[InputSplit]
    // Group by similar input specs, calculate the splits, and try to combine them to minimize small files
    inputs.groupBy(i => (i.formatClass,  i.mapperClass)).foreach({ case (classes, i) =>
      val (formatClass, mapperClass) = classes
      val paths = i.flatMap(_.paths).toArray
      if (paths.length > 0) {
        FileInputFormat.setInputPaths(job, paths:_*)
        val combined = combine(job, formatClass, mapperClass)
        out.addAll(combined.asJava)
      }
    })
    out
  }

  /* Currently this only does a single level of combination at the node level, this means we always get
     the best locality, but not always the minimal splits. */
  def combine(job: Job, formatClass: Class[_ <: InputFormat[_, _]], mapperClass: Class[_ <: CombinableMapper[_, _, _, _]]): List[InputSplit] = {
    val configuration = job.getConfiguration
    val format = ReflectionUtils.newInstance(formatClass, configuration).asInstanceOf[InputFormat[_, _]]
    val limit = getLimit(configuration)
    val splits = format.getSplits(job).asScala.toArray
    val combined = ProxyInputFormat.chunk(splits, limit)
    combined.map(a => new ProxyInputSplit(a, formatClass, mapperClass, configuration))
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[SplitKey[A], B] = {
    val proxy = split.asInstanceOf[ProxyInputSplit]
    val format = ReflectionUtils.newInstance(proxy.formatClass, context.getConfiguration).asInstanceOf[InputFormat[A, B]]
    new ProxyRecordReader(proxy, format, context)
  }
}

object ProxyInputFormat {
  private val local = new java.util.concurrent.ConcurrentHashMap[String, List[InputSpecification]]

  val InputsKey = ThriftCache.Key("ivory-proxy-inputs")

  def take(context: MrContext): Option[List[InputSpecification]] =
    Option(local.remove(context.id.value))

  def configure(context: MrContext, job: Job, specifications: List[InputSpecification]): Unit = {
    local.put(context.id.value, specifications)
    job.setInputFormatClass(classOf[ProxyInputFormat[_, _]])
    job.setMapperClass(classOf[ProxyMapper[_, _, _, _]])
  }

  /* Group items on the same node and bucket small partitions together. */
  def chunk(splits: Array[InputSplit], limit: Long): List[Array[InputSplit]] =
    splits.groupBy(_.getLocations.mkString("|")).values.toList.flatMap(v => {
      val sorted = v.sortBy(_.getLength)
      val (small, ok) = sorted.partition(_.getLength < limit)
      val extra = small.map(_.getLength).sum
      val buckets = (extra / limit).toInt + 1
      val combined =
        if (extra != 0)
          Partition.partitionGreedily(small.toVector, buckets, (x: InputSplit) => x.getLength).map(_.toArray)
        else
          Vector()
      combined ++ ok.map(s => Array(s))
    })
}
