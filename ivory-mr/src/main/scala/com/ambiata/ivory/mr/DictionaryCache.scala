package com.ambiata.ivory.mr

import com.ambiata.ivory.core.{Crash, Dictionary}
import com.ambiata.ivory.core.thrift.{ThriftDictionary, DictionaryThriftConversion}
import com.ambiata.poacher.mr.ThriftCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

object DictionaryCache {

  val thriftCacheKey = ThriftCache.Key("dictionary")

  def store(job: Job, thriftCache: ThriftCache, dictionary: Dictionary): Unit =
    thriftCache.push(job, thriftCacheKey, DictionaryThriftConversion.dictionaryToThrift(dictionary))

  def load(conf: Configuration, thriftCache: ThriftCache): Dictionary = {
    val dictThrift = new ThriftDictionary
    thriftCache.pop(conf, thriftCacheKey, dictThrift)
    DictionaryThriftConversion.dictionaryFromThrift(dictThrift).fold(m => Crash.error(Crash.Serialization, m), identity)
  }
}
