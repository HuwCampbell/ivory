package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact.{Factsets, FeatureStoreGlob}
import com.ambiata.ivory.storage.arbitraries._
import com.ambiata.ivory.storage.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.manifest.SnapshotManifest
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.notion.core.{Key, KeyName}

import org.specs2._
import org.specs2.execute.AsResult
import org.specs2.matcher.Matcher
import com.ambiata.mundane.testing.RIOMatcher._

import scalaz._, Scalaz._

class SnapshotMetadataStorageSpec extends Specification with ScalaCheck { def is = s2"""

SnapshotManifest Properties
---------------------------

  newManifestFromIdentifier loads what save saves.                      $newSaveLoad

  We define the "latest" snapshot before a date1 as the greatest element having a date <= date1 $latest

  The latest snapshot is considered the "latest up to date" snapshot for date1 if:
    - latestSnapshot.getFeatureStoreId == latestFeatureStore
    - and the snapshot.date == date1

    - OR the snapshot.date != date1
      but there are no partitions between the snapshot date and date1 for factsets in the latest feature store

  then the snapshot is up to date $uptodate

  Any invalid ids should be ignored $ignored

"""

  def newSaveLoad = prop((ns: SnapshotManifest) =>
    RepositoryBuilder.using((repo: Repository) =>
      for {
        _ <- writeSnapshotsAndCommits(repo, ns)
        x <- SnapshotMetadataStorage.latestSnapshot(repo, ns.date).run
      } yield x.map(_.id)) must beOkValue(Some(ns.snapshot))).set(minTestsOk = 3)

  // Replace with better tests when RepositoryScenario is merged in.

  def latest = propNoShrink { (snapshots: SnapshotManifests, date1: Date) =>
    RepositoryBuilder.using { repo =>
      for {
        _                <- snapshots.metas.traverse(writeSnapshotsAndCommits(repo, _))
        latestBeforeDate =  SnapshotMetadataStorage.sort(snapshots.metas.filter(_.date <= date1)).lastOption
        snapshot         <- SnapshotMetadataStorage.latestSnapshot(repo, date1).run
      } yield snapshot.map(_.id) must_== latestBeforeDate.map(_.snapshot)
    } must beOkResult
  }

  def uptodate = propNoShrink { (snapshots: SnapshotManifests, dates: UniqueDates) =>
    RepositoryBuilder.using { repo =>
      val factsetDate = dates.later

      for {
        // Write out some empty commits to match the ids
        _         <- RepositoryBuilder.createDictionary(repo, Dictionary.empty)
        _         <- snapshots.metas.traverse(writeSnapshotsAndCommits(repo, _))
        factsetId <- Factsets.allocateFactsetId(repo)
        _         <- repo.store.utf8.write(Repository.factset(factsetId) / KeyName.unsafe("ns") / Key.unsafe(factsetDate.slashed) / KeyName.unsafe("part"), "content")
        r         <- RepositoryRead.fromRepository(repo)
        store     <- Metadata.incrementFeatureStore(List(factsetId)).run(r)
        _         <- RepositoryBuilder.writeFactsetVersion(List(factsetId)).run(r)
        snapshot  <- SnapshotMetadataStorage.latestUpToDateSnapshot(repo, dates.now).run

      } yield snapshot must beUpToDate(repo, dates.now)

    } must beOkResult
  }.set(minTestsOk = 10)

  def ignored = prop { (snapshots: SnapshotManifests, date1: Date) =>
    RepositoryBuilder.using { repo =>
      for {
        _                <- RepositoryBuilder.createDictionary(repo, Dictionary.empty)
        _                <- snapshots.metas.traverse(writeSnapshotsAndCommits(repo, _))
        _                <- repo.store.utf8.write(Repository.snapshots / "broke" / "n", "random stuff")
        latestBeforeDate =  SnapshotMetadataStorage.sort(snapshots.metas.filter(_.date <= date1)).lastOption
        snapshot         <- SnapshotMetadataStorage.latestSnapshot(repo, date1).run
      } yield snapshot.map(_.id) must_== latestBeforeDate.map(_.snapshot)
    } must beOkResult
  }

  def beUpToDate(repo: Repository, date1: Date): Matcher[Option[SnapshotMetadata]] = { snapshot: Option[SnapshotMetadata] =>
    snapshot must beSome(
      (meta: SnapshotMetadata) => {

        "the snapshot feature store is the latest feature store if it is up to date" ==> {
          Metadata.latestFeatureStoreOrFail(repo).map((store: FeatureStore) => meta.storeId ==== store.id) must beOk
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

  def writeSnapshotsAndCommits(repo: Repository, manifest: SnapshotManifest): RIO[Unit] =
    manifest.storeOrCommit.right.toOption
      .traverseU(cid => CommitTextStorage.storeCommitToId(repo, cid, Commit(DictionaryId.initial, FeatureStoreId.initial, None))).void >>
      SnapshotManifest.io(repo, manifest.snapshot).write(manifest)


  def beOkResult[R : AsResult]: Matcher[RIO[R]] = (resultTIO: RIO[R]) =>
    resultTIO must beOkLike { r =>
      val result = AsResult(r)
      result.isSuccess aka result.message must beTrue
    }
}
