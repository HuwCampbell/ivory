package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact.{Factsets, FeatureStoreGlob}
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.notion.core.{KeyName, Key}


import org.specs2._
import org.scalacheck._, Arbitrary._, Arbitraries._
import org.specs2.execute.AsResult
import org.specs2.matcher.{Matcher, ThrownExpectations}
import com.ambiata.mundane.testing.ResultTIOMatcher._

import scalaz._, Scalaz._, effect.IO
import argonaut._, Argonaut._

class SnapshotManifestSpec extends Specification with ScalaCheck { def is = s2"""

NewSnapshotManifest Properties
------------------------------

  Encode/Decode Json is symmetric                                       $encodeDecodeJson
  newManifestFromIdentifier fails on missing .metadata.json             $newmissing
  newManifestFromIdentifier loads what save saves.                      $newSaveLoad
  NewSnapshotManifests are sorted based on:
    snapshotId, date and commitId                                       $newManifestSorting

SnapshotManifest Properties
---------------------------

  fromIdentifier will not fail on missing .snapmeta and .metadata.json  $missing
  fromIdentifier loads Legacy Snapshot Metadata manifests               $loadLegacy
  fromIdentifier loads New Snapshot Metadata manifests                  $loadNew
  Legacy SnapshotManifests are ordered before new ones                  $legacyLessThanNew
  Legacy SnapshotManifests are ordered using Legacy Snapshot ordering   $legacysorting
  New SnapshotManifests are ordered using NewSnapshotManifest ordering  $newsorting

  We define the "latest" snapshot before a date1 as the greatest element having a date <= date1 $latest

  The latest snapshot is considered the "latest up to date" snapshot for date1 if:
    - latestSnapshot.getFeatureStoreId == latestFeatureStore
    - and the snapshot.date == date1

    - OR the snapshot.date != date1
      but there are no partitions between the snapshot date and date1 for factsets in the latest feature store

  then the snapshot is up to date $uptodate

"""

  def encodeDecodeJson = prop((ns: NewSnapshotManifest) =>
    Parse.decodeEither[NewSnapshotManifest](ns.asJson.nospaces) must_== ns.right)

  def newmissing = prop((id: SnapshotId) =>
    RepositoryBuilder.using((repo: Repository) =>
      NewSnapshotManifest.newManifestFromIdentifier(repo, id)) must beFail).set(minTestsOk = 1)

  def newSaveLoad = prop((ns: NewSnapshotManifest) =>
    RepositoryBuilder.using((repo: Repository) =>
      for {
        _ <- NewSnapshotManifest.save(repo, ns)
        x <- NewSnapshotManifest.newManifestFromIdentifier(repo, ns.snapshotId)
      } yield x) must beOkValue(ns)).set(minTestsOk = 3)

  def newManifestSorting = prop((metas: List[NewSnapshotManifest]) =>
    metas.sorted must_== metas.sortBy(nm => (nm.snapshotId, nm.date, nm.commitId)))

  def missing = prop((id: SnapshotId) =>
    RepositoryBuilder.using((repo: Repository) =>
      SnapshotManifest.fromIdentifier(repo, id).run) must beOkValue(None)).set(minTestsOk = 1)

  def loadLegacy = prop((lm: SnapshotMeta) =>
    RepositoryBuilder.using((repo: Repository) =>
      (for {
        _ <- SnapshotMeta.save(repo, lm).liftM[OptionT]
        x <- SnapshotManifest.fromIdentifier(repo, lm.snapshotId)
      } yield x).run) must beOkValue(SnapshotManifest.snapshotManifestLegacy(lm).some)).set(minTestsOk = 3)

  def loadNew = prop((nm: NewSnapshotManifest) =>
    RepositoryBuilder.using((repo: Repository) =>
      (for {
        _ <- NewSnapshotManifest.save(repo, nm).liftM[OptionT]
        x <- SnapshotManifest.fromIdentifier(repo, nm.snapshotId)
      } yield x).run) must beOkValue(SnapshotManifest.snapshotManifestNew(nm).some)).set(minTestsOk = 3)

  def legacyLessThanNew = prop((lm: SnapshotMeta, nm: NewSnapshotManifest) =>
    SnapshotManifest.snapshotManifestLegacy(lm) < SnapshotManifest.snapshotManifestNew(nm))

  def legacysorting = prop((lms: List[SnapshotMeta]) =>
    lms.map(SnapshotManifest.snapshotManifestLegacy).sorted must_== lms.sorted.map(SnapshotManifest.snapshotManifestLegacy))

  def newsorting = prop((nms: List[NewSnapshotManifest]) =>
    nms.map(SnapshotManifest.snapshotManifestNew).sorted must_== nms.sorted.map(SnapshotManifest.snapshotManifestNew))

  // Replace with better tests when RepositoryScenario is merged in.

  def latest = prop { (snapshots: SnapshotManifestList, date1: Date) =>
    RepositoryBuilder.using { repo =>
      for {
        _                <- snapshots.metas.traverse(storeSnapshotManifest(repo, _))
        latestBeforeDate =  snapshots.metas.filter(_.date <= date1).sorted.lastOption
        snapshot         <- SnapshotManifest.latestSnapshot(repo, date1).run
      } yield snapshot must_== latestBeforeDate
    } must beOkResult
  }

  // This only checks the legacy snapshot manifests, as the new ones will involve looking up the commit in the metadata,
  // which wont be in the repo built.
  def uptodate = prop { (snapshots: SnapshotMetaList, date1: Date, dateOffset: DateOffset) =>
    RepositoryBuilder.using { repo =>
      val factsetDate = dateOffset.offset(date1)

      for {
        _         <- snapshots.metas.traverse((lm: SnapshotMeta) => storeSnapshotManifest(repo, SnapshotManifest.snapshotManifestLegacy(lm)))
        factsetId <- Factsets.allocateFactsetId(repo)
        _         <- repo.store.utf8.write(Repository.factset(factsetId) / "ns" / Key.unsafe(factsetDate.slashed) / "part", "content")
        store     <- Metadata.incrementFeatureStore(List(factsetId)).run(IvoryRead.testing(repo))
        _         <- writeFactsetVersion(repo, List(factsetId))
        snapshot  <- SnapshotManifest.latestUpToDateSnapshot(repo, date1).run
      } yield snapshot must beUpToDate(repo, date1)

    } must beOkResult
  }.set(minTestsOk = 10)

  def storeSnapshotManifest(repo: Repository, meta: SnapshotManifest): ResultTIO[Unit] = meta.fold(SnapshotMeta.save(repo, _), NewSnapshotManifest.save(repo, _))

  def beUpToDate(repo: Repository, date1: Date): Matcher[Option[SnapshotManifest]] = { snapshot: Option[SnapshotManifest] =>
    snapshot must beSome(
      (meta: SnapshotManifest) => {

        "the snapshot feature store is the latest feature store if it is up to date" ==> {
          Metadata.latestFeatureStoreOrFail(repo).flatMap((store: FeatureStore) => SnapshotManifest.getFeatureStoreId(repo, meta).map(_ ==== store.id)) must beOk
        }

        "there are no new facts after the snapshot date" ==> {
          val newFacts =
            for {
              store      <- Metadata.latestFeatureStoreOrFail(repo)
              partitions <- FeatureStoreGlob.strictlyAfterAndBefore(repo, store, meta.date, date1).map(_.partitions)
            } yield partitions

          newFacts must beOkLike(_ must beEmpty)
        }
      }
    ) or { snapshot must beNone }
  }

  def beOkResult[R : AsResult]: Matcher[ResultTIO[R]] = (resultTIO: ResultTIO[R]) =>
    resultTIO must beOkLike { r =>
      val result = AsResult(r)
      result.isSuccess aka result.message must beTrue
    }
}
