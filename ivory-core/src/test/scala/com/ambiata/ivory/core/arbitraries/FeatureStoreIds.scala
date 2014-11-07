package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._


case class FeatureStoreIds(ids: List[FeatureStoreId])

object FeatureStoreIds {
  implicit def FeatureStoreIdsArbitrary: Arbitrary[FeatureStoreIds] =
    Arbitrary(GenIdentifier.stores.map(FeatureStoreIds.apply))
}
