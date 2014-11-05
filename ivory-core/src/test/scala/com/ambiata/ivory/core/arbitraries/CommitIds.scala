package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._


case class CommitIds(ids: List[CommitId])

object CommitIds {
  implicit def CommitIdsArbitrary: Arbitrary[CommitIds] =
    Arbitrary(GenIdentifier.commits.map(CommitIds.apply))
}
