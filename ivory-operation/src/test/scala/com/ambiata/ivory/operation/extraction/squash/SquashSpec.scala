package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.Snapshot
import com.ambiata.ivory.operation.extraction.squash.SquashArbitraries._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.control._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import IvorySyntax._

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

    RepositoryBuilder.using { repo => for {
      _ <- RepositoryBuilder.createRepo(repo, dict, List(allFacts))
      s <- Snapshot.takeSnapshot(repo, sf.date, false)
      f <- SquashJob.squashFromSnapshotWith(repo, dict, s)(p =>
        ResultT.ok(valueFromSequenceFile[Fact]((repo.root </> p.path).path).run(repo.scoobiConfiguration).toList))
    } yield f
    }.map(postProcess) must beOkValue(
      postProcess(sf.facts.list.flatMap(_.expectedFactsWithCount))
    )
  }).set(minTestsOk = 1)
}
