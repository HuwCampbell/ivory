package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._


class FeatureWindowsSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[FeatureWindows]}

Properties
----------

  `hasWindows` is based on the underlying feature windows:

    ${ prop((w: FeatureWindows) => w.hasWindows ==== w.features.exists(!_.windows.isEmpty)) }

  `byNamespace` is just a convenience for `FeatureRanges#byNamespace`:

    ${ prop((w: FeatureWindows, d: Date) => w.byNamespace(d) ==== Ranges.toNamespaces(w.byFeature(d))) }

  `byFeature` records `to` date accurately for all ranges:

    ${ prop((w: FeatureWindows, d: Date) => w.byFeature(d).values.forall(_.to === d)) }

  `byFeature` records `from` date accurately for all ranges:

    ${ prop((w: FeatureWindows, d: Date) =>
      w.byFeature(d).values.flatMap(_.froms).toSet ==== w.features.flatMap(_.windows).map(Window.startingDate(_, d)).toSet) }

"""
}
