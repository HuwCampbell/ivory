package com.ambiata.ivory.storage.manifest

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.ArgonautProperties._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._
import scalaz.scalacheck.ScalaCheckBinding._


class FactsetManifestSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Encode/Decode Json                           ${encodedecode[FactsetManifest]}
  Equal                                        ${equal.laws[FactsetManifest]}

"""
  implicit def FactsetManifestArbitrary: Arbitrary[FactsetManifest] =
    Arbitrary((arbitrary[FactsetId] |@|
               arbitrary[FactsetFormat] |@|
               arbitrary[List[Partition]])(FactsetManifest(VersionManifest.current, _, _, _)))
}
