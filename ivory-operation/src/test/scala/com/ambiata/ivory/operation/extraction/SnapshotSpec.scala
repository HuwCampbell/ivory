package com.ambiata.ivory.operation.extraction

import org.specs2._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import org.joda.time.LocalDate

class SnapshotSpec extends Specification with SampleFacts { def is = s2"""

  A snapshot of the features can be extracted as a sequence file $e1

"""

  def e1 =
    RepositoryBuilder.using { repo => for {
      _ <- RepositoryBuilder.createRepo(repo, sampleDictionary, sampleFacts)
      _ <- Snapshot.takeSnapshot(repo, Date.fromLocalDate(LocalDate.now), incremental = false)
    } yield ()} must beOk
}
