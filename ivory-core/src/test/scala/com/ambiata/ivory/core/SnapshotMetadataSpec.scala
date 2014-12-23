package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class SnapshotMetadataSpec extends Specification with ScalaCheck { def is = s2"""

Constructors
------------

  This doesn't really test anything, but I am leaving it here so that it
  is more likely for someone to add specs if they add a combinator:

    ${ prop((id: SnapshotId, date: Date, store: FeatureStoreId, dict: Option[DictionaryId]) => {
         val s = SnapshotMetadata(id, date, store, dict)
         (s.id, s.date, s.storeId, s.dictionaryId) ==== ((id, date, store, dict)) }) }

"""
}
