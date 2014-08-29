package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

import org.specs2._
import org.scalacheck._, Arbitrary._, Arbitraries._
import com.ambiata.ivory.data.{Identifier, OldIdentifier}
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.ambiata.mundane.testing.ResultTIOMatcher._

import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._
import org.specs2.execute.AsResult

import scalaz._, Scalaz._
import scalaz.effect.IO
import scalaz.scalacheck.ScalaCheckBinding._

object SnapshotMetaSpec extends Specification with ScalaCheck { def is = s2"""

  ${"""Can find the latest snapshot on hdfs:
       - Take greatest store with duplicate dates
       - Take greatest date with duplicate feature stores
       - Take greatest snapshot id with duplicate SnapshopMeta's"""!e1}
  Sorting SnapshotMeta works on date then store order            $e2
                                                                 """

  implicit def SnapshotMetaArbitrary: Arbitrary[SnapshotMeta] = Arbitrary(for {
    date  <- arbitrary[Date]
    store <- arbitrary[FeatureStoreId]
  } yield SnapshotMeta(date, store))

  def genSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[(SnapshotMeta, SnapshotId)]] =
    ids.ids.traverseU(id => arbitrary[SnapshotMeta].map((_, id)))

  def genSameDateSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[(SnapshotMeta, SnapshotId)]] = for {
    date  <- arbitrary[Date]
    snaps <- ids.ids.traverseU(id => arbitrary[FeatureStoreId].map(sid => (SnapshotMeta(date, sid), id)))
  } yield snaps

  def genSameStoreSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[(SnapshotMeta, SnapshotId)]] = for {
    store <- arbitrary[FeatureStoreId]
    snaps <- ids.ids.traverseU(id => arbitrary[Date].map(d => (SnapshotMeta(d, store), id)))
  } yield snaps

  def genSameSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[(SnapshotMeta, SnapshotId)]] = for {
    date  <- arbitrary[Date]
    store <- arbitrary[FeatureStoreId]
    snaps <- ids.ids.map(id => (SnapshotMeta(date, store), id))
  } yield snaps

  case class Snapshots(snaps: List[(SnapshotMeta, SnapshotId)])
  implicit def SnapshotsArbitrary: Arbitrary[Snapshots] = Arbitrary(for {
    ids <- arbitrary[SmallSnapshotIdList]
    sms <- Gen.oneOf(genSnapshotMetas(ids) // random SnapshotMeta's
      , genSameDateSnapshotMetas(ids) // same date in all SnapshotMeta's
      , genSameStoreSnapshotMetas(ids) // same store in all SnapshotMeta's
      , genSameSnapshotMetas(ids)) // same SnapshotMeta's
  } yield Snapshots(sms))

  def e1 = prop { (snaps: Snapshots, date: Date) =>
    createRepo { repo =>
      val expected = snaps.snaps.filter(_._1.date <= date).sorted.lastOption.map(_.swap)

      (snaps.snaps.traverse({ case (meta, id) => storeSnapshotMeta(repo, id, meta) }) must beOk) and
        (SnapshotMeta.latest(repo, date) must beOkValue(expected))
    }

  } // TODO This takes around 30 seconds, needs investigation

  def e2 =
    prop((snaps: List[SnapshotMeta])          => assertSortOrder(snaps)) and
    prop((d: Date, ids: List[FeatureStoreId]) => assertSortOrder(ids.map(id => SnapshotMeta(d, id))))

  def createRepo[R : AsResult](f: Repository => R): org.specs2.execute.Result = {
    val sc: ScoobiConfiguration = scoobiConfiguration
    Temporary.using { dir =>
      val repo = Repository.fromHdfsPath(dir </> "repo", sc)
      ResultT.ok[IO, org.specs2.execute.Result](AsResult(f(repo)))
    } must beOkLike(_.isSuccess)
  }

  def storeSnapshotMeta(repo: Repository, id: SnapshotId, meta: SnapshotMeta): ResultTIO[Unit] = {
    val path = Repository.snapshots </> FilePath(id.render) </> SnapshotMeta.fname
    repo.toReference(path).run(store => p => store.linesUtf8.write(p, meta.stringLines))
  }

  def assertSortOrder(sms: List[SnapshotMeta]) =
    sms.sorted must_== sms.sortBy(sm => (sm.date, sm.featureStoreId))
}
