package com.ambiata.ivory.core

import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._, Arbitraries._

import scalaz._, Scalaz._

class FeatureStoreIdSpec extends Specification with ScalaCheck { def is = s2"""

Feature Store Id Properties
---------------------

  Render/parse is symmetric                       $symmetric
  Initial starts with zero                        $initial
  If next succeeds identifier is always larger    $next
  Can sort                                        $sort

"""

  def symmetric = prop((id: FeatureStoreId) =>
    FeatureStoreId.parse(id.render) must_== Some(id))

  def initial =
    Some(FeatureStoreId.initial) must_== FeatureStoreId.parse("00000")

  def next = prop((id: FeatureStoreId) =>
    id.next.forall(_ > id))

  def sort = prop((ids: List[FeatureStoreId]) =>
    ids.sorted must_== ids.sortBy(_.id))
}
