package com.ambiata.ivory.core

import scalaz._

case class Commit(
  dictionaryId: DictionaryId
, dictionary: Dictionary
, store: FeatureStore
, config: Option[(RepositoryConfigId, RepositoryConfig)]
)

object Commit {
  implicit def CommitEqual: Equal[Commit] =
    Equal.equalA
}
