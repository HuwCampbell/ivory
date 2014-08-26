package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.{Factsets, FeatureStoreGlob}
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.control._

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

 SnapshotMeta objects are sorted based on:
   snapshotId, date and storeId $sorting

 We define the "latest" snapshot before a date1 as the greatest element having a date <= date1 $latest

 The latest snapshot is considered the "latest up to date" snapshot for date1
   if no facts have been ingested after the snapshot date and before date1 $uptodate

"""

  def sorting = prop { snaps: List[SnapshotMeta] =>
    snaps.sorted must_== snaps.sortBy(sm => (sm.snapshotId, sm.date, sm.featureStoreId))
  }

  def latest = prop { (snapshots: Snapshots, date1: Date) =>
    val repository = createRepository("SnapshotMetaSpec.latest")
    snapshots.metas.traverse(storeSnapshotMeta(repository, _)) must beOk

    val latestBeforeDate = snapshots.metas.filter(_.date <= date1).sorted.lastOption

    SnapshotMeta.latestSnapshot(repository, date1) must beOkLike { snapshot =>
      snapshot must_== latestBeforeDate
    }
  }

  def uptodate = prop { (snapshots: Snapshots, date1: Date, dateOffset: DateOffset) =>
    val repository = createRepository("SnapshotMetaSpec.uptodate")
    val factsetDate = dateOffset.offset(date1)

    val createFactset =
      for {
        _         <- snapshots.metas.traverse(storeSnapshotMeta(repository, _))
        factsetId <- Factsets.allocateFactsetId(repository)
        _         <- repository.toStore.utf8.write(Repository.factset(factsetId) </> "ns" </> FilePath(factsetDate.slashed) </> "part", "content")
        store     <- Metadata.incrementFeatureStore(factsetId).run(IvoryRead.testing(repository))
        _         <- writeFactsetVersion(repository, List(factsetId))

      } yield ()

    createFactset must beOk

    SnapshotMeta.latestUpToDateSnapshot(repository, date1) must beOkLike { snapshot =>
      snapshot must beSome { meta: SnapshotMeta =>
        val newFacts =
          for {
            store      <- Metadata.latestFeatureStoreOrFail(repository)
            partitions <- FeatureStoreGlob.strictlyAfterAndBefore(repository, store, meta.date, date1).map(_.partitions)
          } yield partitions

        newFacts must beOkLike(_ must beEmpty)
      } or {
        snapshot must beNone
      }
    }
  }.set(minTestsOk = 10)

  /**
   * ARBITRARIES
   */
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

  case class DateOffset(year: Short, month: Byte, day: Byte) {
    /** offset an existing date to get a new one */
    def offset(date: Date): Date = {
      val newYear  = (date.year + year).toShort
      val newMonth = (math.abs((date.month + month) % 12) + 1).toByte
      val newDay   = (math.abs((date.day + day) % 26) + 1).toByte
      Date.unsafe(newYear, newMonth, newDay)
    }
  }

  /** generate date offsets */
  def genDateOffset: Gen[DateOffset] =
    for {
      y <- Gen.choose[Short](-5, 5)
      m <- Gen.choose[Byte](-12, 12)
      d <- Gen.choose[Byte](-31, 31)
    } yield DateOffset(y, m, d)

  implicit def DateOffsetArbitrary: Arbitrary[DateOffset] =
    Arbitrary(genDateOffset)

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
