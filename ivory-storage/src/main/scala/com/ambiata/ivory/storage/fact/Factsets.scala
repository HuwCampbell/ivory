package com.ambiata.ivory.storage
package fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.manifest.FactsetManifest
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.partition._
import com.ambiata.ivory.storage.version.Version
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
    Partitions.getFromFactset(repository, id).map(partitions => Factset(id, partitions.map(_.value).sorted))

  def updateFeatureStore(factsetId: FactsetId): RepositoryTIO[Option[FeatureStoreId]] = for {
    globs <- RepositoryT.fromRIO(r =>
      // TODO Very very short-term hack. We'll need to do a second sweep to remove the old version completely (which shouldn't be hard)
      Version.write(r, Repository.factset(factsetId), Version(FactsetVersionTwo.toString)) >> FactsetGlob.select(r, factsetId))
    r <- globs.traverseU(g => for {
        fs <- Metadata.incrementFeatureStore(List(factsetId))
        _  <- RepositoryT.fromRIO(r => FactsetManifest.io(r, factsetId).write(FactsetManifest.create(factsetId, FactsetFormat.V2, g.partitions)))
        _  <- Metadata.incrementCommitFeatureStoreT(fs)
      } yield fs)
    } yield r

  // TODO Remove
  def updateFactsetMetadata(repository: Repository, factsetId: FactsetId): RIO[Unit] = for {
    _     <- Version.write(repository, Repository.factset(factsetId), Version(FactsetVersionTwo.toString))
    globs <- FactsetGlob.select(repository, factsetId)
    _     <- globs.traverseU(g => FactsetManifest.io(repository, factsetId).write(FactsetManifest.create(factsetId, FactsetFormat.V2, g.partitions)))
    } yield ()
}
