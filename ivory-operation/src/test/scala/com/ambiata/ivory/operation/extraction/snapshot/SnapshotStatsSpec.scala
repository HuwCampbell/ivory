package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.operation.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties

class SnapshotStatsSpec extends Specification with ScalaCheck { def is = s2"""

  Stats can be saved and loaded                               $loadSave ${tag("store")}
  SnapshotStas Equal laws                                     ${ScalazProperties.equal.laws[SnapshotStats]}
  Stats can be encoded and decoded to JSON                    ${ArgonautProperties.encodedecode[SnapshotStats]}
"""

  def loadSave = prop((local: LocalTemporary, stats: SnapshotStats, snapshotId: SnapshotId) => for {
    d <- local.directory
    r <- RepositoryBuilder.repository
    _ <- SnapshotStats.save(r, snapshotId, stats)
    z <- SnapshotStats.load(r, snapshotId)
  } yield z ==== stats).set(minTestsOk = 10)
}
