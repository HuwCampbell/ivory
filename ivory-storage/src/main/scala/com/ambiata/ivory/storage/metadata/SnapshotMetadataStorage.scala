package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.manifest.SnapshotManifest
import com.ambiata.mundane.control._

import scalaz._, Scalaz._

object SnapshotMetadataStorage {
  def byId(repository: Repository, id: SnapshotId): RIO[Option[SnapshotMetadata]] =
    SnapshotManifest.io(repository, id).read.flatMap(_.traverseU(fromManifest(repository, _)))

  def byIdOrFail(repository: Repository, id: SnapshotId): RIO[SnapshotMetadata] =
    byId(repository, id).flatMap(RIO.fromOption(_, s"Ivory invariant violated, could not locate snapshot metadata for $id"))

  def list(repository: Repository): RIO[List[SnapshotMetadata]] =
    SnapshotStorage.ids(repository).flatMap(_.traverse(byId(repository, _)).map(_.flatten))

  def fromManifest(repo: Repository, meta: SnapshotManifest): RIO[SnapshotMetadata] =
    meta.storeOrCommit.fold(
      s => (s, none).pure[RIO],
      commitId => Metadata.commitFromIvory(repo, commitId).map(c => c.featureStoreId -> c.dictionaryId.some)
    ).map(x => SnapshotMetadata(meta.snapshot, meta.date, x._1, x._2))
}
