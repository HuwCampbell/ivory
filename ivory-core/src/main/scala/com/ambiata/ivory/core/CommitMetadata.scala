package com.ambiata.ivory.core

import scalaz._

case class CommitMetadata(dictionaryId: DictionaryId, featureStoreId: FeatureStoreId, configId: Option[RepositoryConfigId])

object CommitMetadata {
  implicit def CommitMetadataEqual: Equal[CommitMetadata] =
    Equal.equalA
}
