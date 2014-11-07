package com.ambiata.ivory.operation.debug

import org.specs2._, execute.{Failure => SpecsFailure}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.core.{TemporaryType => TT}
import com.ambiata.ivory.core.{TemporaryLocations => T, _}
import com.ambiata.ivory.core.arbitraries._

import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import org.joda.time.LocalDate

class DumpFactsSpec extends Specification with ScalaCheck { def is = s2"""

  We can dump factsets           $factsets              ${tag("mr")}
  We can dump snapshots          $snapshots             ${tag("mr")}
  We can dump both               $both                  ${tag("mr")}

"""
  def factsets = propNoShrink((data: FactsWithDictionary) => {
    T.withIvoryLocationDir(TT.Hdfs) { location =>
      RepositoryBuilder.using { repository => for {
        _ <- RepositoryBuilder.createRepo(repository, data.dictionary, List(data.facts))
        _ <- DumpFacts.dump(repository, DumpFactsRequest(FactsetId.initial :: Nil, Nil, Nil, Nil), location)
        r <- IvoryLocation.readLines(location)
      } yield r }
    } must beOkLike(r =>
      (r.length, r.forall(_.endsWith(s"Factset[${FactsetId.initial.render}]")), data.facts.forall(f => r.exists(l => l.contains(f.entity) && l.contains(f.feature)))) ==== ((data.facts.length, true, true)))
  }).set(minTestsOk = 3)

  def snapshots = prop((data: FactsWithDictionary) => {
    T.withIvoryLocationDir(TT.Hdfs) { location =>
      RepositoryBuilder.using { repository => for {
        _ <- RepositoryBuilder.createRepo(repository, data.dictionary, List(data.facts))
        _ <- Snapshots.takeSnapshot(repository, Date.maxValue)
        _ <- DumpFacts.dump(repository, DumpFactsRequest(Nil, SnapshotId.initial :: Nil, Nil, Nil), location)
        r <- IvoryLocation.readLines(location)
      } yield r }
    } must beOkLike(r => {
      (r.length, r.forall(_.endsWith(s"Snapshot[${SnapshotId.initial.render}]")), data.facts.forall(f => r.exists(l => l.contains(f.entity) && l.contains(f.feature)))) ==== ((data.facts.length, true, true)) })
  }).set(minTestsOk = 3)

  def both = propNoShrink((data: FactsWithDictionary) => {
    T.withIvoryLocationDir(TT.Hdfs) { location =>
      RepositoryBuilder.using { repository => for {
        _ <- RepositoryBuilder.createRepo(repository, data.dictionary, List(data.facts))
        _ <- Snapshots.takeSnapshot(repository, Date.maxValue)
        _ <- DumpFacts.dump(repository, DumpFactsRequest(FactsetId.initial :: Nil, SnapshotId.initial :: Nil, Nil, Nil), location)
        r <- IvoryLocation.readLines(location)
      } yield r}
    } must beOkLike(r =>
      (r.length, r.filter(_.endsWith(s"Snapshot[${SnapshotId.initial.render}]")).size, r.filter(_.endsWith(s"Factset[${FactsetId.initial.render}]")).size) ==== ((data.facts.length * 2, data.facts.length, data.facts.length)))
  }).set(minTestsOk = 3)
}
