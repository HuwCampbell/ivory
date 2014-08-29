package com.ambiata.ivory.operation.extraction

import org.specs2._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._, Arbitraries._
import IvorySyntax._
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWindows
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDate

class SnapshotSpec extends Specification with SampleFacts with ScalaCheck { def is = s2"""

  A snapshot of the features can be extracted as a sequence file $e1
  A snapshot of the features can be extracted over a window      $windowing        ${tag("mr")}

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
        f  = valueFromSequenceFile[Fact](repo.snapshot(s.snapshotId).toHdfs.toString).run(repo.scoobiConfiguration)
      } yield f
    }.map(_.toSet) must beOkValue(facts.toSet)
  }).set(minTestsOk = 1)

}
