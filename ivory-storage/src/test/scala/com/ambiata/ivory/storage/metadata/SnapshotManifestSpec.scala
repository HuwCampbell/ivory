package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.{Factsets, FeatureStoreGlob}
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.Arbitraries._
import com.ambiata.ivory.storage.repository._


import org.specs2._
import org.scalacheck._, Arbitrary._, Arbitraries._
import org.specs2.execute.AsResult
import org.specs2.matcher.{Matcher, ThrownExpectations}
import com.ambiata.mundane.testing.ResultTIOMatcher._

import scalaz._, Scalaz._
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

}
