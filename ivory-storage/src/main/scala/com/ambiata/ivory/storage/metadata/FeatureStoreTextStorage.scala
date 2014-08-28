package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.Factsets
import com.ambiata.mundane.control._

import scalaz.{Value => _, _}, Scalaz._, effect._

object FeatureStoreTextStorage extends TextStorage[Prioritized[FactsetId], List[Prioritized[FactsetId]]] {

  val name = "feature store"

  /** Increment the latest FeatureStore by prepending the given FactsetId and creating a new FeatureStore */
  def increment(repo: Repository, factsetId: FactsetId): ResultTIO[FeatureStore] = for {
    factset     <- Factsets.factset(repo, factsetId)
    latest      <- latestId(repo)
    next        <- ResultT.fromOption[IO, FeatureStoreId](latest.map(_.next).getOrElse(Some(FeatureStoreId.initial)), "Run out of FeatureStore ids!")
    prev        <- latest.traverse(id => fromId(repo, id))
    newFactsets = prev.map(fs => factset +: fs.factsets.map(_.value)).getOrElse(List(factset))
    newStoreO = FeatureStore.fromList(next, newFactsets)
    newStore    <- ResultT.fromOption[IO, FeatureStore](newStoreO, s"Can not add anymore factsets to feature store '${next}'")
    _           <- storeIdsToId(repo, newStore.id, newStore.factsetIds)
  } yield newStore

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
    storeIdsFromReference(repository.toReference(Repository.featureStoreById(id)))

  def storeIdsToId(repository: Repository, id: FeatureStoreId, fstore: List[Prioritized[FactsetId]]): ResultTIO[Unit] =
    storeIdsToReference(repository.toReference(Repository.featureStoreById(id)), fstore)

  def storeIdsFromReference(ref: ReferenceIO): ResultTIO[List[Prioritized[FactsetId]]] =
    ref.run(store => path => store.linesUtf8.read(path).flatMap(lines =>
      ResultT.fromDisjunction[IO, List[Prioritized[FactsetId]]](fromLines(lines.toList).leftMap(\&/.This.apply))))

  def storeIdsToReference(ref: ReferenceIO, fstore: List[Prioritized[FactsetId]]): ResultTIO[Unit] =
    ref.run(store => path => store.linesUtf8.write(path, toList(fstore).map(toLine)))

  def fromList(factsets: List[Prioritized[FactsetId]]): List[Prioritized[FactsetId]] =
    factsets

  def toList(store: List[Prioritized[FactsetId]]): List[Prioritized[FactsetId]] =
    store.sorted

  def parseLine(i: Int, l: String): ValidationNel[String, Prioritized[FactsetId]] = for {
    pri <- Priority.parseInt(i).toSuccess(NonEmptyList(s"Can not parse priority '${i}'"))
    fid <- FactsetId.parse(l).toSuccess(NonEmptyList(s"Can not parse Factset Id '${l}'"))
  } yield Prioritized[FactsetId](pri, fid)

  def toLine(p: Prioritized[FactsetId]): String =
    p.value.render

  def listIds(repo: Repository): ResultTIO[List[FeatureStoreId]] = for {
    paths <- repo.toStore.list(Repository.featureStores).map(_.filterHidden)
    ids   <- paths.traverseU(p =>
               ResultT.fromOption[IO, FeatureStoreId](FeatureStoreId.parse(p.basename.path),
                                                      s"Can not parse Feature Store id '${p}'"))
  } yield ids

  def latestId(repo: Repository): ResultTIO[Option[FeatureStoreId]] =
    listIds(repo).map(_.sorted.lastOption)
}
