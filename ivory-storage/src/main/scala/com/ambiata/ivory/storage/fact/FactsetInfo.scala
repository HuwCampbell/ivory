package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.FactsetLookup

import com.ambiata.mundane.io.FilePath

import com.ambiata.poacher.mr._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputSplit

import scalaz._, Scalaz._

case class FactsetInfo(factsetId: FactsetId, partition: Partition, priority: Priority)

object FactsetInfo {
  def getBaseInfo(path: Path): (FactsetId, Partition) = {
    Factset.parseFile(FilePath.unsafe(path.toString)) match {
      case Success(r) => r
      case Failure(e) => Crash.error(Crash.DataIntegrity, s"Can not parse factset path ${e}")
    }
  }


  def fromMr(thriftCache: ThriftCache, factsetLookupKey: ThriftCache.Key,
             configuration: Configuration, inputSplit: InputSplit): FactsetInfo = {
    val (factsetId, partition) = getBaseInfo(MrContext.getSplitPath(inputSplit))

    val priorityLookup = new FactsetLookup <| (fl => thriftCache.pop(configuration, factsetLookupKey, fl))
    val priority = priorityLookup.priorities.get(factsetId.render)
    FactsetInfo(factsetId, partition, Priority.unsafe(priority))
  }
}
