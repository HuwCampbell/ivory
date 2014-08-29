package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core.Repository
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.Store
import com.ambiata.ivory.core._

import scalaz.Scalaz._

object Repositories {

  def initialPaths(repository: Repository) = List(
    repository.root,
    repository.dictionaries,
    repository.featureStores,
    repository.factsets,
    repository.snapshots,
    repository.errors,
    repository.commits
  )

  def create(repo: Repository): ResultTIO[Unit] = {

    for {
      e <- ReferenceStore.exists(repo.toReference(FilePath(".allocated")))
      r <- ResultT.unless(e, for {
        _     <- initialPaths(repo).traverse(ref => ReferenceStore.writeUtf8(ref </> ".allocated", "")).void
        // Set the initial commit
        dict  <- DictionaryThriftStorage(repo).store(Dictionary.empty)
        store <- FeatureStoreTextStorage.increment(repo, Nil)
        _     <- Metadata.incrementCommit(repo, dict, store)
      } yield ())

    } yield r
  }
}
