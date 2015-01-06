package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.storage.manifest.SnapshotManifest
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._

object SnapshotMetadataStorage {
  val allocated = KeyName.unsafe(".allocated")

  def byId(repository: Repository, id: SnapshotId): RIO[Option[SnapshotMetadata]] = for {
    manifest <- SnapshotManifest.io(repository, id).read
    metadata <- manifest.traverseU(toMetadata(repository, _))
  } yield metadata

  def byIdOrFail(repository: Repository, id: SnapshotId): RIO[SnapshotMetadata] =
    byId(repository, id).flatMap({
      case Some(s) =>
        s.pure[RIO]
      case None =>
        RIO.fail(s"Ivory invariant violated, could not locate snapshot metadata for $id")
    })

  def list(repository: Repository): RIO[List[SnapshotMetadata]] = for {
    ids <- repository.store.listHeads(Repository.snapshots).map(_.filterHidden)
    sids <- ids.flatMap(sid => SnapshotId.parse(sid.name).toList).pure[RIO]
    manifests <- sids.traverseU(SnapshotManifest.io(repository, _).read).map(_.flatten)
    metadatas <- manifests.traverseU(manifest => toMetadata(repository, manifest))
  } yield metadatas

  def createSnapshotManifest(repo: Repository, date: Date): RIO[SnapshotManifest] = for {
    snapshotId <- allocateId(repo)
    commitId <- Metadata.findOrCreateLatestCommitId(repo)
  } yield SnapshotManifest.createLatest(commitId, snapshotId, date)

  def toMetadata(repo: Repository, meta: SnapshotManifest): RIO[SnapshotMetadata] =
    meta.storeOrCommit.fold(
      s => (s, none).pure[RIO],
      commitId => Metadata.commitFromIvory(repo, commitId).map(c => c.featureStoreId -> c.dictionaryId.some)
    ).map(x => SnapshotMetadata(meta.snapshot, meta.date, x._1, x._2))

  /**
   * create a new Snapshot id by create a new .allocated sub-directory
   * with the latest available identifier + 1
   */
  private def allocateId(repo: Repository): RIO[SnapshotId] =
    IdentifierStorage.write(repo, Repository.snapshots, allocated, scodec.bits.ByteVector.empty).map(SnapshotId.apply)
}
