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
import scalaz.scalacheck.ScalaCheckBinding._

object SnapshotMetaSpec extends Specification with ScalaCheck { def is = s2"""

  Can find the latest snapshot on hdfs                   $e1
  Sorting SnapshotMeta works on date then store order    $e2
                                                         """

  implicit def SnapshotMetaArbitrary: Arbitrary[SnapshotMeta] = Arbitrary(for {
    date <- arbitrary[Date]
    s    <- Gen.choose(1, Priority.Max.toShort)
  } yield SnapshotMeta(date, fatrepo.ImportWorkflow.zeroPad(s)))

  case class Snapshots(snaps: List[(SnapshotMeta, Identifier)])
  implicit def SnapshotsArbitrary: Arbitrary[Snapshots] = Arbitrary(for {
    ids <- arbitrary[IdentifierList]
    sms <- ids.ids.traverseU(id => arbitrary[SnapshotMeta].map((_, id)))
  } yield Snapshots(sms))

  def e1 = prop((snaps: Snapshots, date: Date) => {
    val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir("SnapshotMetaSpec.e1").getPath)
    val repo = Repository.fromHdfsPath(base </> "repo", sc)

    val expected = snaps.snaps.filter(_._1.date <= date).sortBy(_._1).lastOption.map({ case (meta, id) =>
      (repo.toReference(Repository.snapshots </> FilePath(id.render)), meta)
    })

    (snaps.snaps.traverse({ case (meta, id) =>
      val path = Repository.snapshots </> FilePath(id.render) </> SnapshotMeta.fname
      repo.toReference(path).run(store => p => store.linesUtf8.write(p, meta.stringLines))
    }) must beOk) and (SnapshotMeta.latest(repo.toReference(Repository.snapshots), date) must beOkValue(expected))
  }) // TODO This takes around 30 seconds, needs investigation

  def e2 =
    prop((snaps: List[SnapshotMeta])    => assertSortOrder(snaps)) and
    prop((d: Date, ids: OldIdentifiers) => assertSortOrder(ids.ids.map(SnapshotMeta(d, _))))

  def assertSortOrder(sms: List[SnapshotMeta]) =
    sms.sorted must_== sms.sortBy(sm => (sm.date, sm.store))
}
