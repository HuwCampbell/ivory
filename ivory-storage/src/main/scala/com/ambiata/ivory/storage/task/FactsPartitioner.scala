package com.ambiata.ivory.storage.task

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.ReducerLookup
import com.ambiata.ivory.storage.lookup.ReducerLookups
import com.ambiata.poacher.mr.MrContext
import org.apache.hadoop.conf.{Configuration, Configurable}
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.Partitioner

/**
 * Partitioner for facts
 *
 * Keys are partitioned by the externalized feature id (held in the top 32 bits of the key)
 * into predetermined buckets. We use the predetermined buckets as upfront knowledge of
 * the input size is used to reduce skew on input data.
 */
class FactsPartitioner extends BaseFactsPartitioner[BytesWritable] {
  def getFeatureId(k: BytesWritable): FeatureIdIndex =
    FactsetWritable.getFeatureId(k)
  def getHash(k: BytesWritable): Int =
    // Currently this is the date, but at some point we want to switch between date and entity depending on the feature
    // https://github.com/ambiata/ivory/issues/455
    FactsetWritable.getDate(k).int
}

trait BaseFactsPartitioner[A] extends Partitioner[A, BytesWritable] with Configurable {
  var _conf: Configuration = null
  var ctx: MrContext = null
  val lookup = new ReducerLookup

  def setConf(conf: Configuration): Unit = {
    _conf = conf
    ctx = MrContext.fromConfiguration(_conf)
    ctx.thriftCache.pop(conf, ReducerLookups.Keys.ReducerLookup, lookup)
  }

  def getConf: Configuration =
    _conf

  def getPartition(k: A, v: BytesWritable, partitions: Int): Int = {
    val offset = lookup.reducers.get(getFeatureId(k).int)
    FeatureReducerOffset.getReducer(offset, getHash(k)) % partitions
  }

  def getFeatureId(k: A): FeatureIdIndex
  def getHash(k: A): Int
}
