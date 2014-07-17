package com.ambiata.ivory.storage.metadata

import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._

import scalaz.{Value => _, _}, Scalaz._, effect._

object FeatureStoreTextStorage extends TextStorage[PrioritizedFactset, FeatureStore] {

  val name = "feature store"

  def fromName(repository: Repository, name: String): ResultTIO[FeatureStore] =
    fromReference(repository.toReference(Repository.storeByName(name)))

  def toName(repository: Repository, name: String, fstore: FeatureStore): ResultTIO[Unit] =
    toReference(repository.toReference(Repository.storeByName(name)), fstore)

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
    pri.map(p => PrioritizedFactset(Factset(l), p).success)
       .getOrElse("Given list of factset ids is too large to fit into a FeatureStore!".failureNel)
  }

  def toLine(f: PrioritizedFactset): String =
    f.set.name

  def fromFactsets(sets: List[Factset]): FeatureStore =
    FeatureStore(PrioritizedFactset.fromFactsets(sets))
}
