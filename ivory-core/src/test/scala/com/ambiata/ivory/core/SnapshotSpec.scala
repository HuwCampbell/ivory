package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import scalaz._, scalacheck.ScalazArbitrary._

class SnapshotSpec extends Specification with ScalaCheck { def is = s2"""

Constructors
------------

  This doesn't really test anything, but I am leaving it here so that it
  is more likely for someone to add specs if they add a combinator:

    ${ prop((id: SnapshotId, date: Date, store: FeatureStore, dictionary: Option[Identified[DictionaryId, Dictionary]], bytes: Bytes \/ List[Sized[Namespace]], format: SnapshotFormat) => {
         val s = Snapshot(id, date, store, dictionary, bytes, format)
         (s.id, s.date, s.store, s.dictionary, s.bytes, s.format) ==== ((id, date, store, dictionary, bytes, format)) }) }

"""
}
