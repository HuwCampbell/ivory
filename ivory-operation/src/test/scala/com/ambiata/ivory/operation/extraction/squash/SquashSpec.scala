package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.ClusterTemporary._
import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.ivory.operation.extraction.squash.SquashArbitraries._
import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import scalaz.effect.IO

class SquashSpec extends Specification with ScalaCheck { def is = s2"""

  A count of the facts can be squashed out of a snapshot      $count  ${tag("mr")}
  A dump of reductions can be squashed out of a snapshot      $dump   ${tag("mr")}

"""

  def count = propNoShrink((sf: SquashFactsMultiple) => sf.hasVirtual ==> {
    def postProcess(results: List[Fact]): List[Fact] =
      results.sortBy(fact => (fact.entity, fact.featureId))

    val expectedFacts: List[Fact] = sf.facts.list.flatMap(_.expectedFactsWithCount)

    withCluster { cluster =>
      RepositoryBuilder.using { repo => for {
        _   <- RepositoryBuilder.createRepo(repo, sf.dict, List(sf.allFacts))
        s   <- Snapshots.takeSnapshot(repo, IvoryFlags.default, sf.date)
        out = OutputDataset.fromIvoryLocation(repo.toIvoryLocation(Key(KeyName.unsafe("out"))))
        key <- SquashJob.squashFromSnapshotWith(repo, s, SquashConfig.testing, cluster)
        f   <- RIO.safe[List[Fact]](postProcess(valueFromSequenceFile[Fact](key._1.hdfsPath.toString).run(repo.scoobiConfiguration).toList))
      } yield f }
    } must beOkValue(postProcess(expectedFacts))
  }).set(minTestsOk = 1, maxDiscardRatio = 10)

  def dump = prop((sf: SquashFactsMultiple) => sf.hasVirtual ==> {
    // Take a subset of the entities and virtual features (one from each SquashFacts)
    // Note that it's possible to generate the same entity for different features
    val entityKeys = sf.facts.list.map(_.facts.head.entity).toSet
    val entities: Map[String, List[FeatureId]] = sf.facts.list
      .flatMap(f => (f.facts.head.entity :: f.facts.list.map(_.entity).filter(entityKeys.contains))
        .flatMap(e => f.dict.cg.virtual.headOption.map(e -> _._1))
      ).groupBy(_._1).mapValues(_.map(_._2))
    RepositoryBuilder.using { repo => for {
      _    <- RepositoryBuilder.createRepo(repo, sf.dict, List(sf.allFacts))
      s    <- Snapshots.takeSnapshot(repo, IvoryFlags.default, sf.date)
      out  =  repo.toIvoryLocation(Key(KeyName.unsafe("dump")))
      _    <- SquashDumpJob.dump(repo, s.id, out, entities.values.flatten.toList, entities.keys.toList)
      dump <- IvoryLocation.readLines(out).map(_.map(_.split("\\|", -1) match {
        case Array(e, ns, a, _, _, _) =>  e -> FeatureId(Namespace.unsafe(ns), a)
      }).toSet)
    } yield dump
    } must beOkValue(sf.allFacts.flatMap(f => entities.get(f.entity).toList.flatten.map(f.entity ->)).toSet)
  }).set(minTestsOk = 3, maxDiscardRatio = 10)
}
