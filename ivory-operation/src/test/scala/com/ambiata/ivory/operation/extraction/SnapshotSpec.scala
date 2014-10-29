package com.ambiata.ivory.operation.extraction

import org.specs2._, execute.{Failure => SpecsFailure}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._, Arbitraries._

import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWindows
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDate

import scalaz._, Scalaz._

class SnapshotSpec extends Specification with SampleFacts with ScalaCheck { def is = s2"""

  A snapshot of the features can be extracted as a sequence file $e1
  A snapshot of the features can be extracted over a window      $windowing        ${tag("mr")}
  A snapshot builds incrementally on the last if it exists       $incremental

"""

  def e1 =
    RepositoryBuilder.using { repo => for {
      _ <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
      _ <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), incremental = false)
    } yield ()} must beOk

  def windowing = prop((vdict: VirtualDictionaryWindow, fact: Fact) => {
    val facts = List(
      fact,
      fact.withTime(Time.unsafe(fact.time.seconds + 1)),
      fact.withDate(SnapshotWindows.startingDate(vdict.window, fact.date))
    ).map(_.withFeatureId(vdict.vdict.vd.source))

    val oldfacts = List(
      fact.withDate(Date.fromLocalDate(SnapshotWindows.startingDate(vdict.window, fact.date).localDate.minusDays(1)))
    ).map(_.withFeatureId(vdict.vdict.vd.source))

    RepositoryBuilder.using { repo => for {
        _ <- RepositoryBuilder.createRepo(repo, vdict.vd.dictionary, List(facts ++ oldfacts))
        s <- Snapshot.takeSnapshot(repo, fact.date, false)
        f  = valueFromSequenceFile[Fact](repo.toIvoryLocation(Repository.snapshot(s.meta.snapshotId)).toHdfs).run(repo.scoobiConfiguration)
      } yield f
    }.map(_.toSet) must beOkValue((oldfacts.sortBy(_.date).lastOption.toList ++ facts).toSet)
  }).set(minTestsOk = 1)

  def incremental = prop(
    (vdict: VirtualDictionaryWindow, fact: Fact) => {
      val facts = List(fact).map(_.withFeatureId(vdict.vdict.vd.source))
      val facts2 = List(fact.withDate(Date.fromLocalDate(fact.date.localDate.plusDays(1)))).map(_.withFeatureId(vdict.vdict.vd.source))
      RepositoryBuilder.using((repo: HdfsRepository) =>
        for {
          _ <- RepositoryBuilder.createRepo(repo, vdict.vd.dictionary, facts.pure[List])
          res <- Snapshot.takeSnapshot(repo, fact.date, true)
          _ <- RepositoryBuilder.createFactset(repo, facts2)
          res2 <- Snapshot.takeSnapshot(repo, fact.date, true)
        } yield (res, res2.incremental.map(_.snapshotId))) must beOkLike(
          (t: (SnapshotJobSummary[SnapshotManifest], Option[SnapshotId])) =>
            t._2.cata((sid: SnapshotId) => sid must_== t._1.meta.snapshotId, SpecsFailure("No incremental was used in the second snapshot")))
    }).set(minTestsOk = 1)

}
