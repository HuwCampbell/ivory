package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._, arbitraries._, Arbitraries._
import com.ambiata.ivory.operation.model._
import com.ambiata.ivory.storage.metadata.FeatureIdMappingsStorage
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.mundane.control._

import org.specs2._
import org.specs2.matcher.MatchResult

import scalaz._, Scalaz._

class SnapshotsSpec extends Specification with ScalaCheck { def is = s2"""

  Snapshot respects state priority
    $snapshotStatePriority ${tag("mr")}

  Snapshot keeps all set records
    $snapshotSetPriority ${tag("mr")}

  Snapshot keeps all keyed set records
    $snapshotKeyedSetPriority ${tag("mr")}

  A snapshot contains a mapping of FeatureIds
    $featureMapping ${tag("mr")}

"""

  def snapshotStatePriority = prop((c: ConcreteGroupFact, facts: FactsWithStatePriority, d2: Date) =>
    run(c.onDefinition(_.copy(mode = Mode.State)).dictionary, facts.factsets(c.fact), facts.values.head._1.date max d2)
  ).set(minTestsOk = 5)

  def snapshotSetPriority = prop((c: ConcreteGroupFact, facts: FactsWithStatePriority, d2: Date) =>
    run(c.onDefinition(_.copy(mode = Mode.Set)).dictionary, facts.factsets(c.fact), facts.values.head._1.date max d2)
  ).set(minTestsOk = 5)

  def snapshotKeyedSetPriority = prop((c: ConcreteGroupFact, facts: FactsWithKeyedSetPriority, d2: Date) =>
    run(c.onDefinition(facts.definition).dictionary, facts.factsets(c.fact), facts.dates.head max d2)
  ).set(minTestsOk = 5)

  def run(dictionary: Dictionary, facts: List[List[Fact]], d: Date): RIO[MatchResult[List[Fact]]] = {
    RepositoryBuilder.using { repo => for {
      _ <- RepositoryBuilder.createRepo(repo, dictionary, facts)
      s <- Snapshots.takeSnapshot(repo, IvoryFlags.default, d)
      o = SnapshotLoader.readV2(repo.toIvoryLocation(Repository.snapshot(s.id)).toHdfsPath, repo.scoobiConfiguration)
      e = SnapshotModel.run(FactModel.factsPriority(facts), SnapshotModelConf(d, dictionary))
    } yield o.sorted(Fact.orderEntityDateTime.toScalaOrdering) ==== e.sorted(Fact.orderEntityDateTime.toScalaOrdering)
    }
  }

  def featureMapping = prop((facts: FactsWithDictionary) =>
    (for {
      repo <- RepositoryBuilder.repository
      _    <- RepositoryBuilder.createRepo(repo, facts.dictionary, List(facts.facts))
      s    <- Snapshots.takeSnapshot(repo, IvoryFlags.default, facts.facts.map(_.date).max)
      m    <- FeatureIdMappingsStorage.fromKeyStore(repo, Repository.snapshot(s.id) / FeatureIdMappingsStorage.keyname)
    } yield m.featureIds) must beOkValue(FeatureIdMappings.fromDictionary(facts.dictionary).featureIds)
  ).set(minTestsOk = 3)
}
