package com.ambiata.ivory.core

import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._, Arbitraries._

import scalaz._, Scalaz._

class SnapshotIdSpec extends Specification with ScalaCheck { def is = s2"""

Snapshot Id Properties
---------------------

  Render/parse is symmetric                       $symmetric
  Initial starts with zero                        $initial
  If next succeeds, identifier is always larger   $next
  Can sort                                        $sort

"""

  def symmetric = prop((id: SnapshotId) =>
    SnapshotId.parse(id.render) must_== Some(id))

  def initial =
    Some(SnapshotId.initial) must_== SnapshotId.parse("00000000")

  def next = prop((id: SnapshotId) =>
    id.next.forall(_ > id))

  def sort = prop((ids: List[SnapshotId]) =>
    ids.sorted must_== ids.sortBy(_.id))
}
