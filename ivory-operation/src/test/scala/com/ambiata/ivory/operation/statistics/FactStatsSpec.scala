package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._, arbitraries._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.ivory.storage.statistics._

import org.specs2._
import org.specs2.execute.{Result, AsResult}

import com.ambiata.mundane.testing.RIOMatcher._

import com.ambiata.notion.core._

import com.ambiata.poacher.hdfs.Hdfs

import scalaz.effect.IO

class FactStatsSpec extends Specification with ScalaCheck { def is = sequential ^ s2"""

  FactStats can create statistics for a factset           $factset

  """

  def factset = prop((facts: FactsWithDictionary, nan: Double) => {
    (for {
      repo   <- RepositoryBuilder.repository
      _      <- RepositoryBuilder.createRepo(repo, facts.dictionary, List(facts.facts))
      stats1 <- FactStats.factset(repo, FactsetId.initial)
      stats2 <- FactStatisticsStorage.fromKeyStore(repo, Repository.factset(FactsetId.initial) / "_stats")
    } yield (stats1, stats2)) must beOkLike({ case (s1, s2) =>
      (replaceNaN(s1, nan) ==== replaceNaN(s2, nan)) and (s1.stats.isEmpty ==== false)
    })
  }).set(minTestsOk = 5)

  def replaceNaN(stats: FactStatistics, replacement: Double): FactStatistics = {
    val numerical = FactStatistics.fromNumerical(stats.numerical.map(n => if(n.sqsum.isNaN) n.copy(sqsum = replacement) else n))
    val categorical = FactStatistics.fromCategorical(stats.categorical)
    numerical +++ categorical
  }
}
