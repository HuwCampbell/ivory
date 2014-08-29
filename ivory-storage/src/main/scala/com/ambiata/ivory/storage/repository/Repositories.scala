package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core.Repository
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._
import com.ambiata.mundane.store.Store

import scalaz.Scalaz._
import scalaz.effect.IO

object Repositories {
  def create(repo: Repository): ResultTIO[Unit] = {
    val store: Store[ResultTIO] = repo.toStore
    for {
      e <- store.exists(Repository.root </> ".allocated")
      r <- if (e) ResultT.unit[IO]
      else for {
        _ <- List(
          Repository.root,
          Repository.dictionaries,
          Repository.featureStores,
          Repository.factsets,
          Repository.snapshots,
          Repository.errors,
          Repository.commits
        ).traverse(p => store.utf8.write(p </> ".allocated", "")).void

        // Set the initial commit
        diid <- DictionaryThriftStorage(repo).store(Dictionary.empty)
        fst <- FeatureStoreTextStorage.increment(repo, FactsetId.initial)
        _ <- CommitTextStorage.increment(repo, Commit(DictionaryId(diid._1), fst.id))
      } yield ()
    } yield r
  }
}
