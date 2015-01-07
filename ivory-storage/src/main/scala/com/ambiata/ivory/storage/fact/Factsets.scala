package com.ambiata.ivory.storage
package fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.partition._
import com.ambiata.ivory.storage.manifest.FactsetManifest
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import scalaz._, Scalaz._

object Factsets {
  def listIds(repository: Repository): RIO[List[FactsetId]] = for {
    names <- repository.store.listHeads(Repository.factsets).map(_.filterHidden.map(_.name))
    fids  <- names.traverseU(n => RIO.fromOption[FactsetId](FactsetId.parse(n), s"Can not parse factset id '$n'"))
  } yield fids

  def latestId(repository: Repository): RIO[Option[FactsetId]] =
    listIds(repository).map(_.sorted.lastOption)

  // TODO handle locking
  def allocateFactsetIdI(repository: Repository): IvoryTIO[FactsetId] =
    IvoryT.fromRIO { allocateFactsetId(repository) }

  def allocateFactsetId(repository: Repository): RIO[FactsetId] = for {
    nextOpt <- latestId(repository).map(_.map(_.next).getOrElse(Some(FactsetId.initial)))
    next    <- RIO.fromOption[FactsetId](nextOpt, s"No more Factset Ids left!")
    _       <- repository.store.bytes.write(Repository.factsets / next.asKeyName / ".allocated", scodec.bits.ByteVector.empty)
  } yield next

  def factsets(repository: Repository): RIO[List[Factset]] = for {
    ids      <- listIds(repository)
    factsets <- ids.traverse(id => factset(repository, id))
  } yield factsets.sortBy(_.id)

  def factset(repository: Repository, id: FactsetId): RIO[Factset] =
    FactsetManifest.io(repository, id).readOrFail.map(manifest =>
      Factset(id, manifest.format, manifest.partitions.sorted))

  def updateFeatureStore(factsetId: FactsetId): RepositoryTIO[Option[FeatureStoreId]] =
    RepositoryT.fromRIO(r => Partitions.scrapeFromFactset(r, factsetId)).flatMap(partitions =>
      if (partitions.isEmpty)
        none[FeatureStoreId].pure[RepositoryTIO]
      else for {
          fs <- Metadata.incrementFeatureStore(List(factsetId))
          _  <- RepositoryT.fromRIO(r => FactsetManifest.io(r, factsetId).write(FactsetManifest.create(factsetId, FactsetFormat.V2, partitions)))
          _  <- Metadata.incrementCommitFeatureStoreT(fs)
        } yield fs.some)

  def updateFactsetMetadata(repository: Repository, factsetId: FactsetId): RIO[Unit] =
    Partitions.scrapeFromFactset(repository, factsetId).flatMap(partitions =>
      if (partitions.isEmpty)
        RIO.unit
      else
        FactsetManifest.io(repository, factsetId).write(FactsetManifest.create(factsetId, FactsetFormat.V2, partitions)))
}
