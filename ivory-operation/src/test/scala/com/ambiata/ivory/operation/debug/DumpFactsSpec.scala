package com.ambiata.ivory.operation.debug

import org.specs2._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core.{TemporaryType => TT}
import com.ambiata.ivory.core.{TemporaryLocations => T, _}
import com.ambiata.ivory.core.arbitraries._

import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.ivory.storage.repository.RepositoryBuilder

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
    val facts = uniqueEntities(data.facts)
    T.withIvoryLocationDir(TT.Hdfs) { location =>
      RepositoryBuilder.using { repository => for {
        _ <- RepositoryBuilder.createRepo(repository, removeVirtualFeatures(data.dictionary), List(data.facts))
        _ <- Snapshots.takeSnapshot(repository, IvoryFlags.default, Date.maxValue)
        _ <- DumpFacts.dump(repository, DumpFactsRequest(Nil, SnapshotId.initial :: Nil, Nil, Nil), location)
        r <- IvoryLocation.readLines(location)
      } yield r }
    } must beOkLike(r => {
      (r.length, r.forall(_.endsWith(s"Snapshot[${SnapshotId.initial.render}]")), data.facts.forall(f => r.exists(l => l.contains(f.entity) && l.contains(f.feature)))) ==== ((facts.length, true, true)) })
  }).set(minTestsOk = 3)

  def both = propNoShrink((data: FactsWithDictionary) => {
    val facts = uniqueEntities(data.facts)
    T.withIvoryLocationDir(TT.Hdfs) { location =>
      RepositoryBuilder.using { repository => for {
        _ <- RepositoryBuilder.createRepo(repository, removeVirtualFeatures(data.dictionary), List(facts))
        _ <- Snapshots.takeSnapshot(repository, IvoryFlags.default, Date.maxValue)
        _ <- DumpFacts.dump(repository, DumpFactsRequest(FactsetId.initial :: Nil, SnapshotId.initial :: Nil, Nil, Nil), location)
        r <- IvoryLocation.readLines(location)
      } yield r}
    } must beOkLike(r =>
      (r.length, r.filter(_.endsWith(s"Snapshot[${SnapshotId.initial.render}]")).size, r.filter(_.endsWith(s"Factset[${FactsetId.initial.render}]")).size) ==== ((facts.length * 2, facts.length, facts.length)))
  }).set(minTestsOk = 3)

  // We're not here to test the snapshot logic, which involves complex windowing and priority
  def uniqueEntities(facts: List[Fact]): List[Fact] =
    facts.groupBy(_.entity).values.toList.flatMap(_.headOption)

  def removeVirtualFeatures(dictionary: Dictionary): Dictionary =
    Dictionary(dictionary.definitions.filter({
      case Concrete(_, _) => true
      case Virtual(_, _)  => false
    }))
}
