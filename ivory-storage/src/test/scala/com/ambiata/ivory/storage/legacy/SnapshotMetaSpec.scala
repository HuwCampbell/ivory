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
import com.ambiata.mundane.control._

import scala.util.Sorting
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._

object SnapshotMetaSpec extends Specification with ScalaCheck { def is = s2"""

  ${"""Can find the latest snapshot on hdfs:
       - Take greatest store with duplicate dates
       - Take greatest date with duplicate stores
       - Take greatest snapshot id with duplicate SnapshopMeta's"""!e1}
  Sorting SnapshotMeta works on date then store order            $e2
                                                                 """

  implicit def SnapshotMetaArbitrary: Arbitrary[SnapshotMeta] = Arbitrary(for {
    date  <- arbitrary[Date]
    store <- arbitrary[OldIdentifier]
  } yield SnapshotMeta(date, store.id))

  def genSnapshotMetas(ids: IdentifierList): Gen[List[(SnapshotMeta, Identifier)]] =
    ids.ids.traverseU(id => arbitrary[SnapshotMeta].map((_, id)))

  def genSameDateSnapshotMetas(ids: IdentifierList): Gen[List[(SnapshotMeta, Identifier)]] = for {
    date  <- arbitrary[Date]
    snaps <- ids.ids.traverseU(id => arbitrary[OldIdentifier].map(sid => (SnapshotMeta(date, sid.id), id)))
  } yield snaps

  def genSameStoreSnapshotMetas(ids: IdentifierList): Gen[List[(SnapshotMeta, Identifier)]] = for {
    store <- arbitrary[OldIdentifier]
    snaps <- ids.ids.traverseU(id => arbitrary[Date].map(d => (SnapshotMeta(d, store.id), id)))
  } yield snaps

  def genSameSnapshotMetas(ids: IdentifierList): Gen[List[(SnapshotMeta, Identifier)]] = for {
    date  <- arbitrary[Date]
    store <- arbitrary[OldIdentifier]
    snaps <- ids.ids.map(id => (SnapshotMeta(date, store.id), id))
  } yield snaps

  case class Snapshots(snaps: List[(SnapshotMeta, Identifier)])
  implicit def SnapshotsArbitrary: Arbitrary[Snapshots] = Arbitrary(for {
    ids <- arbitrary[IdentifierList]
    sms <- Gen.oneOf(genSnapshotMetas(ids) // random SnapshotMeta's
                   , genSameDateSnapshotMetas(ids) // same date in all SnapshotMeta's
                   , genSameStoreSnapshotMetas(ids) // same store in all SnapshotMeta's
                   , genSameSnapshotMetas(ids)) // same SnapshotMeta's
  } yield Snapshots(sms))

  def e1 = prop((snaps: Snapshots, date: Date) => {
    val repo = createRepo("SnapshotMetaSpec.e1")

    val expected = snaps.snaps.filter(_._1.date <= date).sorted.lastOption.map(_.swap)

    (snaps.snaps.traverse({ case (meta, id) => storeSnapshotMeta(repo, id, meta) }) must beOk) and
    (SnapshotMeta.latest(repo, date) must beOkValue(expected))
  }) // TODO This takes around 30 seconds, needs investigation

  def e2 =
    prop((snaps: List[SnapshotMeta])    => assertSortOrder(snaps)) and
    prop((d: Date, ids: OldIdentifiers) => assertSortOrder(ids.ids.map(SnapshotMeta(d, _))))

  def createRepo(prefix: String): Repository = {
    val sc: ScoobiConfiguration = scoobiConfiguration

    val base = FilePath(TempFiles.createTempDir(prefix).getPath)
    Repository.fromHdfsPath(base </> "repo", sc)
  }

  def storeSnapshotMeta(repo: Repository, id: Identifier, meta: SnapshotMeta): ResultTIO[Unit] = {
    val path = Repository.snapshots </> FilePath(id.render) </> SnapshotMeta.fname
    repo.toReference(path).run(store => p => store.linesUtf8.write(p, meta.stringLines))
  }

  def assertSortOrder(sms: List[SnapshotMeta]) =
    sms.sorted must_== sms.sortBy(sm => (sm.date, sm.store))
}
