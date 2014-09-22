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
import com.ambiata.mundane.store._
import com.ambiata.mundane.control._
import org.specs2.execute.AsResult
import org.specs2.matcher.{Matcher, ThrownExpectations}
import scalaz._, Scalaz._
import scalaz.effect.IO
import scalaz.scalacheck.ScalaCheckBinding._

object SnapshotMetaSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

 SnapshotMeta objects are sorted based on:
   snapshotId, date and storeId $sorting

 We define the "latest" snapshot before a date1 as the greatest element having a date <= date1 $latest

 The latest snapshot is considered the "latest up to date" snapshot for date1 if:

   - latestSnapshot.featureStore == latestFeatureStore
     - and the snapshot.date == date1

     - OR the snapshot.date != date1
       but there are no partitions between the snapshot date and date1 for factsets in the latest feature store

   then the snapshot is up to date $uptodate
"""

  def sorting = prop { snaps: List[SnapshotMeta] =>
    snaps.sorted must_== snaps.sortBy(sm => (sm.snapshotId, sm.date, sm.featureStoreId))
  }

  def latest = prop { (snapshots: Snapshots, date1: Date) =>
    RepositoryBuilder.using { repository =>

      for {
        _                <- snapshots.metas.traverse(storeSnapshotMeta(repository, _))
        latestBeforeDate =  snapshots.metas.filter(_.date <= date1).sorted.lastOption
        snapshot         <- SnapshotMeta.latestSnapshot(repository, date1)
      } yield snapshot must_== latestBeforeDate
    } must beOkResult
  }

  def uptodate = prop { (snapshots: Snapshots, date1: Date, dateOffset: DateOffset) =>
    RepositoryBuilder.using { repository =>
      val factsetDate = dateOffset.offset(date1)

      for {
        _         <- snapshots.metas.traverse(storeSnapshotMeta(repository, _))
        factsetId <- Factsets.allocateFactsetId(repository)
        _         <- repository.store.utf8.write(Repository.factset(factsetId) / "ns" / Key.unsafe(factsetDate.slashed) / "part", "content")
        store     <- Metadata.incrementFeatureStore(List(factsetId)).run(IvoryRead.testing(repository))
        _         <- writeFactsetVersion(repository, List(factsetId))
        snapshot  <- SnapshotMeta.latestUpToDateSnapshot(repository, date1)
      } yield snapshot must beUpToDate(repository, date1)

    } must beOkResult
  }.set(minTestsOk = 10)

  def beUpToDate(repository: Repository, date1: Date): Matcher[Option[SnapshotMeta]] = { snapshot: Option[SnapshotMeta] =>
    snapshot must beSome { meta: SnapshotMeta =>

      "the snapshot feature store is the latest feature store if it is up to date" ==> {
        Metadata.latestFeatureStoreOrFail(repository) map { store => meta.featureStoreId ==== store.id } must beOk
      }

      "there are no new facts after the snapshot date" ==> {
        val newFacts =
          for {
            store      <- Metadata.latestFeatureStoreOrFail(repository)
            partitions <- FeatureStoreGlob.strictlyAfterAndBefore(repository, store, meta.date, date1).map(_.partitions)
          } yield partitions

        newFacts must beOkLike(_ must beEmpty)
      }
    } or { snapshot must beNone }
  }

  def beOkResult[R : AsResult]: Matcher[ResultTIO[R]] = (resultTIO: ResultTIO[R]) =>
    resultTIO must beOkLike { r =>
      val result = AsResult(r)
      result.isSuccess aka result.message must beTrue
    }

  /**
   * ARBITRARIES
   */
  implicit def SnapshotMetaArbitrary: Arbitrary[SnapshotMeta] = Arbitrary(for {
    snapshotId  <- arbitrary[SnapshotId]
    date        <- arbitrary[Date]
    storeId     <- arbitrary[FeatureStoreId]
    commitId    <- arbitrary[Option[CommitId]]
  } yield SnapshotMeta(snapshotId, date, storeId, commitId))

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
        cid   <- arbitrary[Option[CommitId]]
      } yield SnapshotMeta(id, date, store, cid)
    }

  def genSameDateSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    date  <- arbitrary[Date]
    cid   <- arbitrary[Option[CommitId]]
    snaps <- ids.ids.traverseU(id => arbitrary[FeatureStoreId].map(sid => SnapshotMeta(id, date, sid, cid)))
  } yield snaps

  def genSameStoreSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    store <- arbitrary[FeatureStoreId]
    cid   <- arbitrary[Option[CommitId]]
    snaps <- ids.ids.traverseU(id => arbitrary[Date].map(d => SnapshotMeta(id, d, store, cid)))
  } yield snaps

  def genSameSnapshotMetas(ids: SmallSnapshotIdList): Gen[List[SnapshotMeta]] = for {
    date  <- arbitrary[Date]
    store <- arbitrary[FeatureStoreId]
    cid   <- arbitrary[Option[CommitId]]
    snaps <- ids.ids.map(id => SnapshotMeta(id, date, store, cid))
  } yield snaps

  case class DateOffset(year: Short, month: Byte, day: Byte) {
    import math._

    /** offset an existing date to get a new one */
    def offset(date: Date): Date = {
      val newYear  = (date.year + year).toShort
      val newMonth = (abs((date.month + month) % 12) + 1).toByte
      val newDay   = (abs((date.day + day) % 31) + 1).toByte

      val newDate = Date.unsafe(newYear, newMonth, newDay)
      if (isValid(newDate)) newDate else date
    }

    /** @return a random date, greater or equal than the passed date */
    def makeGreaterDateThan(date: Date) = {
      val newYear  = (date.year + abs(year)).toShort
      val newMonth = { val m = date.month + abs(month); (if (m >= 12) 12 else m).toByte }
      val newDay   = { val d = date.day + abs(day);     (if (d >= 31) 31 else d).toByte }
      val newDate = Date.unsafe(newYear, newMonth, newDay)
      if (isValid(newDate)) newDate else date
    }

    private def isValid(date: Date) =
      Date.isValid(date.year, date.month, date.day)
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

  def createRepository[R : AsResult](f: Repository => R): org.specs2.execute.Result = {
    val sc: ScoobiConfiguration = scoobiConfiguration
    Temporary.using { dir =>
      val repo = HdfsRepository(dir </> FileName.unsafe("repo"), IvoryConfiguration.fromScoobiConfiguration(sc))
      ResultT.ok[IO, org.specs2.execute.Result](AsResult(f(repo)))
    } must beOkLike(_.isSuccess)
  }

  def storeSnapshotMeta(repo: Repository, meta: SnapshotMeta): ResultTIO[Unit] = {
    val key = Repository.snapshots / meta.snapshotId.asKeyName / SnapshotMeta.metaKeyName
    repo.store.linesUtf8.write(key, meta.stringLines)
  }

}
