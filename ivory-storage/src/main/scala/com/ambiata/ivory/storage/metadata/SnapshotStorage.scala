package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.storage.manifest.SnapshotManifest
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._

object SnapshotStorage {
  def source(repository: Repository): Kleisli[RIO, SnapshotId, Option[Snapshot]] =
    Kleisli[RIO, SnapshotId, Option[Snapshot]](id => SnapshotStorage.byId(repository, id))

  def allocateId(repository: Repository): RIO[SnapshotId] =
    IdentifierStorage.write(repository, Repository.snapshots, ".allocated", scodec.bits.ByteVector.empty).map(SnapshotId.apply)

  def ids(repository: Repository): RIO[List[SnapshotId]] =
    repository.store.listHeads(Repository.snapshots).map(_.filterHidden).map(ids =>
      ids.flatMap(sid => SnapshotId.parse(sid.name).toList))

  def byId(repository: Repository, id: SnapshotId): RIO[Option[Snapshot]] =
    SnapshotMetadataStorage.byId(repository, id).flatMap(_.traverse(byMetadata(repository, _)))

  def byIdOrFail(repository: Repository, id: SnapshotId): RIO[Snapshot] =
    byId(repository, id).flatMap(RIO.fromOption(_, s"Ivory invariant violated, could not locate snapshot data for $id"))

  def list(repository: Repository): RIO[List[Snapshot]] =
    ids(repository).flatMap(_.traverse(byId(repository, _)).map(_.flatten))

  def byMetadata(repository: Repository, metadata: SnapshotMetadata): RIO[Snapshot] = for {
    store <- FeatureStoreTextStorage.fromId(repository, metadata.storeId)
    dictionary <- metadata.dictionaryId.traverseU(id => Metadata.dictionaryFromIvory(repository, id).map(Identified(id, _)))
    manifest <- SnapshotManifest.io(repository, metadata.id).readOrFail
    bytes <- manifest.format match {
      case SnapshotFormat.V1 =>
        size(repository, metadata.id).map(_.left)
      case SnapshotFormat.V2 =>
        sizeNamespaces(repository, metadata.id).map(_.right)
    }
  } yield Snapshot(metadata.id, metadata.date, store, dictionary, bytes, manifest.format)

  /*  This should be coming from metadata, see: https://github.com/ambiata/ivory/issues/556 */
  def sizeKey(repository: Repository, key: Key): RIO[Bytes] =
    IvoryLocation.size(repository.toIvoryLocation(key))

  def sizeNamespaces(repository: Repository, id: SnapshotId): RIO[List[Sized[Namespace]]] =
    repository.store.listHeads(Repository.snapshot(id)).map(_.filterHidden).flatMap(namespaces =>
      namespaces.traverse(nsKey => for {
        bytes <- sizeKey(repository, Repository.snapshot(id) / nsKey)
        ns    <- RIO.fromOption(Namespace.nameFromString(nsKey.name), s"Can not parse namespace '${nsKey.name}'")
      } yield Sized(ns, bytes)))

  def size(repository: Repository, id: SnapshotId): RIO[Bytes] =
    sizeKey(repository, Repository.snapshot(id))

  def location(repository: Repository, snapshot: Snapshot): List[IvoryLocation] = {
    val base: Key = Repository.snapshot(snapshot.id)
    snapshot.bytes match {
      case -\/(_)  => List(repository.toIvoryLocation(base))
      case \/-(bs) => bs.map(s => repository.toIvoryLocation(base / s.value.asKeyName))
    }
  }
}
