package com.ambiata.ivory.storage.metadata

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

object CreateFeatureStore {

  def inRepository(repository: Repository, name: String, sets: List[Factset], existing: Option[String] = None): ResultTIO[Unit] = for {
    store    <- existing.traverse(e => Metadata.storeFromIvory(repository, e))
    tmp       = FeatureStoreTextStorage.fromFactsets(sets)
    newStore  = store.map(fs => tmp concat fs).getOrElse(tmp)
    _        <- Metadata.storeToIvory(repository, newStore, name)
  } yield ()
}
