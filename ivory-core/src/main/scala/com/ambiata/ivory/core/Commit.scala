package com.ambiata.ivory.core

import scalaz._

case class Commit(
  id: CommitId
, dictionary: Identified[DictionaryId, Dictionary]
, store: FeatureStore
, config: Option[Identified[RepositoryConfigId, RepositoryConfig]]
)

object Commit {
  implicit def CommitEqual: Equal[Commit] =
    Equal.equalA
}
