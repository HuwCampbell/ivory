package com.ambiata.ivory.storage.manifest

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.ArgonautProperties._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._


class ChordExtractManifestSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Encode/Decode Json                           ${encodedecode[ChordExtractManifest]}
  Equal                                        ${equal.laws[ChordExtractManifest]}

"""
  implicit def ChordExtractManifestArbitrary: Arbitrary[ChordExtractManifest] =
    Arbitrary(arbitrary[CommitId].map(ChordExtractManifest(VersionManifest.current, _)))
}
