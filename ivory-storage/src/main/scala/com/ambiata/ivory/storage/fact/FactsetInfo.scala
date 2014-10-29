package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.{FactsetLookup, FactsetVersionLookup}

import com.ambiata.mundane.io.FilePath

import com.ambiata.poacher.mr._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputSplit

import scalaz._, Scalaz._

case class FactsetInfo(version: FactsetVersion, factConverter: VersionedFactConverter, priority: Priority)

object FactsetInfo {
  def fromMr(thriftCache: ThriftCache, factsetLookupKey: ThriftCache.Key, factsetVersionLookupKey: ThriftCache.Key,
             configuration: Configuration, inputSplit: InputSplit): FactsetInfo = {
    val path = FilePath.unsafe(MrContext.getSplitPath(inputSplit).toString)
    val (factsetId, partition) = Factset.parseFile(path) match {
      case Success(r) => r
      case Failure(e) => Crash.error(Crash.DataIntegrity, s"Can not parse factset path ${e}")
    }

    val versionLookup = new FactsetVersionLookup <| (fvl => thriftCache.pop(configuration, factsetVersionLookupKey, fvl))
    val rawVersion = versionLookup.versions.get(factsetId.render)
    val factsetVersion = FactsetVersion.fromByte(rawVersion).getOrElse(Crash.error(Crash.DataIntegrity, s"Can not parse factset version '${rawVersion}'"))

    val converter = factsetVersion match {
      case FactsetVersionOne => VersionOneFactConverter(partition)
      case FactsetVersionTwo => VersionTwoFactConverter(partition)
    }
    val priorityLookup = new FactsetLookup <| (fl => thriftCache.pop(configuration, factsetLookupKey, fl))
    val priority = priorityLookup.priorities.get(factsetId.render)
    FactsetInfo(factsetVersion, converter, Priority.unsafe(priority))
  }
}
