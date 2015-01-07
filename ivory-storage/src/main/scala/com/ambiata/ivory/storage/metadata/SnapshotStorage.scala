package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._

object SnapshotStorage {
  def allocateId(repository: Repository): RIO[SnapshotId] =
    IdentifierStorage.write(repository, Repository.snapshots, ".allocated", scodec.bits.ByteVector.empty).map(SnapshotId.apply)

  def ids(repository: Repository): RIO[List[SnapshotId]] =
    repository.store.listHeads(Repository.snapshots).map(_.filterHidden).map(ids =>
      ids.flatMap(sid => SnapshotId.parse(sid.name).toList))

  def byId(repository: Repository, id: SnapshotId): RIO[Option[Snapshot]] =
    SnapshotMetadataStorage.byId(repository, id).flatMap(_.traverse(byMetadata(repository, _)))

  def byIdOrFail(repository: Repository, id: SnapshotId): RIO[Snapshot] =
    byId(repository, id).flatMap({
      case Some(s) =>
        s.pure[RIO]
      case None =>
        RIO.fail(s"Ivory invariant violated, could not locate snapshot data for $id")
    })

  def list(repository: Repository): RIO[List[Snapshot]] =
    ids(repository).flatMap(_.traverse(byId(repository, _)).map(_.flatten))

  def byMetadata(repository: Repository, metadata: SnapshotMetadata): RIO[Snapshot] = for {
    store <- FeatureStoreTextStorage.fromId(repository, metadata.storeId)
    dictionary <- metadata.dictionaryId.traverseU(id => Metadata.dictionaryFromIvory(repository, id).map(Identified(id, _)))
    bytes <- size(repository, metadata.id)
  } yield Snapshot(metadata.id, metadata.date, store, dictionary, bytes)

  /*  This should be coming from metadata, see: https://github.com/ambiata/ivory/issues/556 */
  def size(repository: Repository, id: SnapshotId): RIO[Bytes] =
    IvoryLocation.size(repository.toIvoryLocation(Repository.snapshot(id)))
}
