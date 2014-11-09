package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.ivory.core._

import scalaz.Scalaz._

object Repositories {

  def initialKeys(repository: Repository): List[Key] = List(
    Repository.root,
    Repository.dictionaries,
    Repository.featureStores,
    Repository.factsets,
    Repository.snapshots,
    Repository.errors,
    Repository.commits
  )

  // A very termporary shim to allow incremental adoption of IvoryT.
  def createI(repo: Repository): IvoryTIO[Unit] =
    IvoryT.fromResultTIO { create(repo) }

  def create(repo: Repository): ResultTIO[Unit] = for {
    e <- repo.store.exists(Key(".allocated"))
    r <- ResultT.unless(e, for {
      _     <- initialKeys(repo).traverse(key => repo.store.utf8.write(key / ".allocated", "")).void
      // Set the initial commit
      dict  <- DictionaryThriftStorage(repo).store(Dictionary.empty)
      store <- FeatureStoreTextStorage.increment(repo, Nil)
      _     <- Metadata.incrementCommit(repo, dict, store)
    } yield ())
  } yield r
}
