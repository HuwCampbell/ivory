package com.ambiata.ivory.storage.manifest

import com.ambiata.ivory.core.ArgonautProperties._
import com.ambiata.ivory.storage.arbitraries.Arbitraries._

import org.specs2._

import scalaz.scalacheck.ScalazProperties._


class SnapshotManifestSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Encode/Decode Json                           ${encodedecode[SnapshotManifest]}
  Equal                                        ${equal.laws[SnapshotManifest]}

"""

}
