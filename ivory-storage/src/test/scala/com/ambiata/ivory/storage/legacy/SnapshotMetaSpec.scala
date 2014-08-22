package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

import org.specs2._
import org.scalacheck._, Arbitrary._, Arbitraries._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.ambiata.mundane.testing.ResultTIOMatcher._

import com.nicta.scoobi.Scoobi.ScoobiConfiguration
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._
import org.specs2.execute.AsResult
import org.specs2.matcher.ThrownExpectations

import scalaz._, Scalaz._
import scalaz.effect.IO
import scalaz.scalacheck.ScalaCheckBinding._

object SnapshotMetaSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

  ${"""Can find the latest snapshot on hdfs:
       - Take greatest store with duplicate dates
       - Take greatest date with duplicate stores
       - Take greatest snapshot id with duplicate SnapshopMeta's""" ! latest}
       
  SnapshotMeta objects are sorted based on: snapshotId, date and storeId $sorting
  """

  // TODO This takes around 30 seconds, needs investigation
  def latest = prop { (snapshots: Snapshots, date: Date) =>
    val repository = createRepository("SnapshotMetaSpec.e1")
    snapshots.metas.traverse(storeSnapshotMeta(repository, _)) must beOk

    val expected = snapshots.metas.filter(_.date <= date).sorted.lastOption

    SnapshotMeta.latest(repository, date) must beOkValue(expected)
  }

  def sorting = prop { snaps: List[SnapshotMeta] =>
    snaps.sorted must_== snaps.sortBy(sm => (sm.snapshotId, sm.date, sm.featureStoreId))
  }

  implicit def SnapshotMetaArbitrary: Arbitrary[SnapshotMeta] = Arbitrary(for {
    snapshotId  <- arbitrary[SnapshotId]
    date        <- arbitrary[Date]
    storeId     <- arbitrary[FeatureStoreId]
  } yield SnapshotMeta(snapshotId, date, storeId))

  case class Snapshots(metas: List[SnapshotMeta])
  
  implicit def SnapshotsArbitrary: Arbitrary[Snapshots] = Arbitrary { 
    for {
      ids <- arbitrary[SmallSnapshotIdList]
      sms <- Gen.oneOf(genSnapshotMetas(ids)                // random SnapshotMeta's
                       , genSameDateSnapshotMetas(ids)      // same date in all SnapshotMeta's
                       , genSameStoreSnapshotMetas(ids)     // same store in all SnapshotMeta's
                       , genSameSnapshotMetas(ids))         // same SnapshotMeta's
    } yield Snapshots(sms)
  }

  def genSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] =
    ids.ids.traverseU { id =>
      for {
        date  <- arbitrary[Date]
        store <- arbitrary[FeatureStoreId]
      } yield SnapshotMeta(id, date, store)
    }

  def genSameDateSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    date  <- arbitrary[Date]
    snaps <- ids.ids.traverseU(id => arbitrary[FeatureStoreId].map(sid => SnapshotMeta(id, date, sid)))
  } yield snaps

  def genSameStoreSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    store <- arbitrary[FeatureStoreId]
    snaps <- ids.ids.traverseU(id => arbitrary[Date].map(d => SnapshotMeta(id, d, store)))
  } yield snaps

  def genSameSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    date  <- arbitrary[Date]
    store <- arbitrary[FeatureStoreId]
    snaps <- ids.ids.map(id => SnapshotMeta(id, date, store))
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

  def createRepository[R : AsResult](f: Repository => R): org.specs2.execute.Result = {
    val sc: ScoobiConfiguration = scoobiConfiguration
    Temporary.using { dir =>
      val repo = Repository.fromHdfsPath(dir </> "repo", sc)
      ResultT.ok[IO, org.specs2.execute.Result](AsResult(f(repo)))
    } must beOkLike(_.isSuccess)
  }

  def storeSnapshotMeta(repo: Repository, meta: SnapshotMeta): ResultTIO[Unit] = {
    val path = Repository.snapshots </> FilePath(meta.snapshotId.render) </> SnapshotMeta.fname
    repo.toReference(path).run(store => p => store.linesUtf8.write(p, meta.stringLines))
  }

}
