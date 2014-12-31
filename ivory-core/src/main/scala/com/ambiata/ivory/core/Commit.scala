package com.ambiata.ivory.core

case class Commit(dictionaryId: DictionaryId, dictionary: Dictionary, store: FeatureStore, config: Option[(RepositoryConfigId, RepositoryConfig)])
