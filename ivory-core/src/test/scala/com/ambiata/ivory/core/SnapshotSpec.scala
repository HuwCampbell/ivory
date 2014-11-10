package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class SnapshotSpec extends Specification with ScalaCheck { def is = s2"""

Constructors
------------

  This doesn't really test anything, but I am leaving it here so that it
  is more likely for someone to add specs if they add a combinator:

    ${ prop((id: SnapshotId, date: Date, store: FeatureStore) => {
         val s = Snapshot(id, date, store)
         (s.id, s.date, s.store) ==== ((id, date, store)) }) }

"""
}
