package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

import org.specs2._
import org.scalacheck._, Arbitrary._, Arbitraries._
import com.ambiata.ivory.data.Arbitraries._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.mundane.testing.ResultTIOMatcher._

import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import com.ambiata.mundane.io._

import scalaz._, Scalaz._

object SnapshotMetaSpec extends Specification with ScalaCheck { def is = s2"""

  Can find the latest snapshot on hdfs           $e1
                                                 """

  implicit def SnapshotMetaArbitrary: Arbitrary[SnapshotMeta] = Arbitrary(for {
    date <- arbitrary[Date]
    s    <- Gen.choose(1, Priority.Max.toShort)
  } yield SnapshotMeta(date, fatrepo.ImportWorkflow.zeroPad(s)))

  def e1 = prop((snaps: List[(SnapshotMeta, Identifier)], date: Date) => {
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir("SnapshotMetaSpec.e1").getPath)
    val repo = Repository.fromHdfsPath(base </> "repo", sc)

    val expected = snaps.filter(_._1.date <= date).sortBy(_._1.date).lastOption.map({ case (meta, id) =>
      (repo.toReference(Repository.snapshots </> FilePath(id.render)), meta)
    })

    seqToResult(snaps.map({ case (meta, id) =>
      val path = Repository.snapshots </> FilePath(id.render) </> SnapshotMeta.fname
      repo.toReference(path).run(store => p => store.linesUtf8.write(p, meta.stringLines)) must beOk
    })) and (SnapshotMeta.latest(repo.toReference(Repository.snapshots), date) must beOkValue(expected))
  }).set(minTestsOk = 5) // Not too many runs are it will take a long time
}
