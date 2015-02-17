package com.ambiata.ivory.storage.statistics

import com.ambiata.ivory.core._, FactStasticsCodecs._
import com.ambiata.notion.core._
import com.ambiata.mundane.control._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

object FactStatisticsStorage {

  def toKeyStore(repo: Repository, key: Key, stats: FactStatistics): RIO[Unit] =
    repo.store.utf8.write(key, toJsonString(stats))

  def fromKeyStore(repo: Repository, key: Key): RIO[FactStatistics] =
    repo.store.utf8.read(key).map(fromJsonString).flatMap(RIO.fromDisjunctionString)

  def fromJsonString(str: String): String \/ FactStatistics =
    Parse.decodeEither[FactStatistics](str)

  def toJsonString(stats: FactStatistics): String =
    stats.asJson.nospaces

  def fromJsonLines(lines: List[String]): String \/ FactStatistics =
    lines.traverseU(Parse.decodeEither[FactStatisticsType]).map(FactStatistics.apply)
}
