package com.ambiata.ivory.storage.repository

import com.ambiata.mundane.control._
import com.ambiata.mundane.store.Store

import scalaz.Scalaz._
import scalaz.effect.IO

object Repositories {
  def onStore(repo: Repository): ResultTIO[Unit] = {
    val store: Store[ResultTIO] = repo.toStore
    for {
      e <- store.exists(Repository.root </> ".allocated")
      r <- if (e) ResultT.unit[IO]
      else List(
        Repository.root,
        Repository.dictionaries,
        Repository.featureStores,
        Repository.factsets,
        Repository.snapshots,
        Repository.errors
      ).traverse(p => store.utf8.write(p </> ".allocated", "")).void
    } yield r
  }
}
