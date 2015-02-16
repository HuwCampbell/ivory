package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._, arbitraries._
import com.ambiata.ivory.storage.repository.RepositoryBuilder

import org.specs2._
import org.specs2.execute.{Result, AsResult}

import com.ambiata.mundane.testing.RIOMatcher._

import com.ambiata.notion.core._

import com.ambiata.poacher.hdfs.Hdfs

import scalaz.effect.IO
//import argonaut._, Argonaut._

class FactStatsSpec extends Specification with ScalaCheck { def is = sequential ^ s2"""

  FactStats can create statistics for a factset           $factset

  """

  def factset = prop((facts: FactsWithDictionary) => {
    (for {
      repo  <- RepositoryBuilder.repository
      _     <- RepositoryBuilder.createRepo(repo, facts.dictionary, List(facts.facts))
      _     <- FactStats.factset(repo, FactsetId.initial)
      lines <- Hdfs.globLines(repo.toIvoryLocation(Repository.factset(FactsetId.initial) / "_stats").toHdfsPath, "*").run(repo.configuration)
      _      = lines.foreach(println)
    } yield ()) must beOk
  }).set(minTestsOk = 5)
}
