package com.ambiata.ivory.core

import org.specs2._
import org.scalacheck._, Arbitrary._
import arbitraries.Arbitraries._

import scalaz._, Scalaz._
import argonaut._, Argonaut._

class FeatureStoreIdSpec extends Specification with ScalaCheck { def is = s2"""

Feature Store Id Properties
---------------------

  Render/parse is symmetric                       $symmetric
  Encode/decode Json is symmetric                 $encodeDecodeJson
  Initial starts with zero                        $initial
  If next succeeds identifier is always larger    $next
  Can sort                                        $sort

"""

  def symmetric = prop((id: FeatureStoreId) =>
    FeatureStoreId.parse(id.render) must_== Some(id))

  def encodeDecodeJson = prop((id: FeatureStoreId) =>
    Parse.decodeEither[FeatureStoreId](id.asJson.nospaces) must_== id.right)

  def initial =
    Some(FeatureStoreId.initial) must_== FeatureStoreId.parse("00000000")

  def next = prop((id: FeatureStoreId) =>
    id.next.forall(_ > id))

  def sort = prop((ids: List[FeatureStoreId]) =>
    ids.sorted must_== ids.sortBy(_.id))
}
