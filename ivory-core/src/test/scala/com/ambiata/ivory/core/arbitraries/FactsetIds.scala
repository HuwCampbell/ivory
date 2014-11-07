package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import org.scalacheck._

/* Non-empty, contiguous list of FactsetIds. */
case class FactsetIds(ids: List[FactsetId])

object FactsetIds {
  implicit def FactsetIdsArbitrary: Arbitrary[FactsetIds] =
    Arbitrary(GenIdentifier.factsets.map(FactsetIds.apply))
}
