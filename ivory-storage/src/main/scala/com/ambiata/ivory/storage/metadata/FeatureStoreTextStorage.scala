package com.ambiata.ivory.storage.metadata

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._

import scalaz.{Value => _, _}, Scalaz._, effect._

object FeatureStoreTextStorage extends TextStorage[PrioritizedFactset, FeatureStore] {

  val name = "feature store"

  def fromId(repository: Repository, id: FeatureStoreId): ResultTIO[FeatureStore] =
    fromReference(repository.toReference(Repository.storeById(id)))

  def toId(repository: Repository, id: FeatureStoreId, fstore: FeatureStore): ResultTIO[Unit] =
    toReference(repository.toReference(Repository.storeById(id)), fstore)

  def fromReference(ref: ReferenceIO): ResultTIO[FeatureStore] =
    ref.run(store => path => store.linesUtf8.read(path).flatMap(lines =>
      ResultT.fromDisjunction[IO, FeatureStore](fromLines(lines.toList).leftMap(\&/.This.apply))))

  def toReference(ref: ReferenceIO, fstore: FeatureStore): ResultTIO[Unit] =
    ref.run(store => path => store.linesUtf8.write(path, toList(fstore).map(toLine)))

  def fromList(sets: List[PrioritizedFactset]) =
    FeatureStore(sets)

  def toList(store: FeatureStore): List[PrioritizedFactset] =
    store.factsets.sortBy(_.priority)

  def parseLine(i: Int, l: String): ValidationNel[String, PrioritizedFactset] = {
    val pri = if(i <= Priority.Max.toShort) Priority.create(i.toShort) else None
    pri.map(p => PrioritizedFactset(FactsetId(l), p).success)
       .getOrElse("Given list of factset ids is too large to fit into a FeatureStore!".failureNel)
  }

  def toLine(f: PrioritizedFactset): String =
    f.set.name

  def fromFactsets(sets: List[FactsetId]): FeatureStore =
    FeatureStore(PrioritizedFactset.fromFactsets(sets))

  def listIds(repo: Repository): ResultTIO[List[FeatureStoreId]] = for {
    paths <- repo.toStore.list(Repository.stores)
    ids   <- paths.traverseU(p =>
               ResultT.fromOption[IO, FeatureStoreId](FeatureStoreId.parse(p.basename.path),
                                                      s"Can not parse Feature Store id '${p}'"))
  } yield ids

  def latestId(repo: Repository): ResultTIO[Option[FeatureStoreId]] =
    listIds(repo).map(_.sorted.lastOption)
}
