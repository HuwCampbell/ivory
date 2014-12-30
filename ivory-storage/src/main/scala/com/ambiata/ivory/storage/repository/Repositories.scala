package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.ivory.core._

import scalaz._, Scalaz._

object Repositories {

  val initialKeys: List[Key] = List(
    Repository.root,
    Repository.dictionaries,
    Repository.featureStores,
    Repository.factsets,
    Repository.snapshots,
    Repository.errors,
    Repository.commits
  )

  // A very temporary shim to allow incremental adoption of IvoryT.
  def create(repo: Repository, config: RepositoryConfig): RIO[Unit] =
    IvoryRead.createIO >>= createI(repo, config).run

  def createI(repo: Repository, config: RepositoryConfig): IvoryTIO[Unit] = IvoryT.read[RIO] >>= (read => IvoryT.fromRIO(for {
    e <- repo.store.exists(Key(".allocated"))
    r <- RIO.unless(e, for {
      _     <- initialKeys.traverse(key => repo.store.utf8.write(key / ".allocated", "")).void

      cid   <- RepositoryConfigTextStorage.store(config).toIvoryT(repo).run(read)
      // Set the initial commit
      dict  <- DictionaryThriftStorage(repo).store(Dictionary.empty)
      store <- FeatureStoreTextStorage.increment(repo, Nil)
      _     <- Metadata.incrementCommit(repo, dict, store, cid)
    } yield ())
  } yield r))
}
