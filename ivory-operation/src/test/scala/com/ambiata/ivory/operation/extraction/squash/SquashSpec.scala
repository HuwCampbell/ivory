package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.ivory.operation.extraction.squash.SquashArbitraries._
import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.nicta.scoobi.Scoobi._
import org.specs2._

import scalaz.{Value => _, _}, Scalaz._

class SquashSpec extends Specification with SampleFacts with ScalaCheck { def is = s2"""

  A count of the facts can be squashed out of a snapshot      $count  ${tag("mr")}
"""

  def count = prop((sf: SquashFactsMultiple) => {
    val dict = sf.facts.map(_.dict).foldLeft(Dictionary.empty) {
      // Set the expression for all features to count for simplicity, we test all the expression logic elsewhere
      case (d, vd) => d append vd.withExpression(Count).dictionary
    }
    val allFacts = sf.facts.list.flatMap(_.facts.list)

    def postProcess(results: List[Fact]): List[Fact] =
      results.sortBy(fact => (fact.entity, fact.featureId))

    val expectedFacts = sf.facts.list.flatMap(_.expectedFactsWithCount)
    RepositoryBuilder.using { repo => for {
      _ <- RepositoryBuilder.createRepo(repo, dict, List(allFacts))
      res <- Snapshots.takeSnapshot(repo, sf.date)
      s     = res.meta
      out   = repo.toIvoryLocation(Key(KeyName.unsafe("out"))): IvoryLocation
      f <- SquashJob.squashFromSnapshotWith(repo, s, SquashConfig.testing, List(out))((key, _) =>
        ResultT.safe(postProcess(valueFromSequenceFile[Fact](repo.toIvoryLocation(key).toHdfs)
          .run(repo.scoobiConfiguration).toList))
      )
      p <- IvoryLocation.readLines(out </> FileName.unsafe(".profile"))
    } yield (f, p.size > 0)
    } must beOkValue((
      postProcess(expectedFacts),
      true
    ))
  }).set(minTestsOk = 1, maxDiscardRatio = 10)
}
