package com.ambiata.ivory.storage.statistics

import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._, FactStasticsCodecs._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.notion.core._

import org.specs2._

import argonaut._, Argonaut._
import scalaz._, Scalaz._

class FactStatisticsStorageSpec extends Specification with ScalaCheck { def is = s2"""

FactStatistics Storage Spec
-------------------------------

  Symmetric read / write with Key                              $symmetricKey
  Symmetric to / from json                                     $symmetricJson
  From lines of json statistics snippets                       $jsonLines

"""
  val key: Key = Repository.root / "stats"

  def symmetricKey = propNoShrink((stats: FactStatistics) =>
    (for {
      repo <- RepositoryBuilder.repository
      _    <- FactStatisticsStorage.toKeyStore(repo, key, stats)
      s    <- FactStatisticsStorage.fromKeyStore(repo, key)
    } yield s) must beOkValue(stats)
  ).set(minTestsOk = 20)

  def symmetricJson = prop((stats: FactStatistics) =>
    FactStatisticsStorage.fromJsonString(FactStatisticsStorage.toJsonString(stats)) ==== stats.right)

  def jsonLines = prop((stats: FactStatistics) =>
    FactStatisticsStorage.fromJsonLines(stats.stats.map(_.asJson.toString)) ==== stats.right)
}
