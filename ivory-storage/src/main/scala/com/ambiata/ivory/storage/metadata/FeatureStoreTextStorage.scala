package com.ambiata.ivory.storage
package metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.Factsets
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz.{Value => _, _}, Scalaz._, effect._

object FeatureStoreTextStorage extends TextStorage[Prioritized[FactsetId], List[Prioritized[FactsetId]]] {

  val name = "feature store"

  /** Increment the latest FeatureStore by prepending the given FactsetId and creating a new FeatureStore */
  def increment(repo: Repository, factsetIds: List[FactsetId]): ResultTIO[FeatureStoreId] = for {
    latest      <- latestId(repo)
    next        <- ResultT.fromOption[IO, FeatureStoreId](latest.map(_.next).getOrElse(Some(FeatureStoreId.initial)), "Ran out of FeatureStore ids!")
    prevIds     <- latest.traverse(id => fromId(repo, id))
    newFactsetIds <- ResultT.fromOption[IO, List[Prioritized[FactsetId]]](
                      Prioritized.fromList(prevIds.map(fs =>
                        factsetIds ++ fs.factsets.map(_.value.id)).getOrElse(factsetIds))
                     , "Could not prioritize the factset ids")
    _           <- storeIdsToId(repo, next, newFactsetIds)
  } yield next

  /**
   * Current: At the moment this will list all partitions in every factset
   *          in the feature store.
   *
   * Future: The aim is to store all factsets and its partition information
   *         in metadata attached to each feature store and only read that.
   */
  def fromId(repo: Repository, id: FeatureStoreId): ResultTIO[FeatureStore] = for {
    storeIds <- storeIdsFromId(repo, id)
    factsets <- storeIds.traverse(fid => Factsets.factset(repo, fid.value))
    store    <- ResultT.fromOption[IO, FeatureStore](FeatureStore.fromList(id, factsets), s"Could not parse feature store '${id}'")
  } yield store

  def toId(repo: Repository, featureStore: FeatureStore): ResultTIO[Unit] =
    storeIdsToId(repo, featureStore.id, featureStore.factsetIds)

  def storeIdsFromId(repository: Repository, id: FeatureStoreId): ResultTIO[List[Prioritized[FactsetId]]] =
    storeIdsFromKey(repository, Repository.featureStoreById(id))

  def storeIdsToId(repository: Repository, id: FeatureStoreId, fstore: List[Prioritized[FactsetId]]): ResultTIO[Unit] =
    storeIdsToKey(repository, Repository.featureStoreById(id), fstore)

  def storeIdsFromKey(repository: Repository, key: Key): ResultTIO[List[Prioritized[FactsetId]]] =
    repository.store.linesUtf8.read(key).flatMap(lines =>
      ResultT.fromDisjunction[IO, List[Prioritized[FactsetId]]](fromLines(lines.toList).leftMap(\&/.This.apply)))

  def storeIdsToKey(repository: Repository, key: Key, fstore: List[Prioritized[FactsetId]]): ResultTIO[Unit] =
    repository.store.linesUtf8.write(key, toList(fstore).map(toLine))

  def fromList(factsets: List[Prioritized[FactsetId]]): ValidationNel[String, List[Prioritized[FactsetId]]] =
    Validation.success(factsets)

  def toList(store: List[Prioritized[FactsetId]]): List[Prioritized[FactsetId]] =
    store.sorted

  def parseLine(i: Int, l: String): ValidationNel[String, Prioritized[FactsetId]] =
    (Priority.parseInt(i).toSuccess(NonEmptyList(s"Can not parse priority '${i}'"))
      |@| FactsetId.parse(l).toSuccess(NonEmptyList(s"Can not parse Factset Id '${l}'"))
      )(Prioritized[FactsetId](_, _))

  def toLine(p: Prioritized[FactsetId]): String =
    p.value.render

  def listIds(repository: Repository): ResultTIO[List[FeatureStoreId]] = for {
    paths <- repository.store.list(Repository.featureStores).map(_.filterHidden)
    ids   <- {
    val p = paths
    paths.traverseU(p =>
               ResultT.fromOption[IO, FeatureStoreId](FeatureStoreId.parse(p.name),
                                                      s"Can not parse Feature Store id '${p.name}'"))
    }
  } yield ids

  def latestId(repository: Repository): ResultTIO[Option[FeatureStoreId]] =
    listIds(repository).map(_.sorted.lastOption)
}
