package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._


class RangeSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[Range[FeatureId]]}

Properties
----------

  `from` is always some for a non empty list of `froms`:

    ${ prop((r: Range[FeatureId]) => !r.froms.isEmpty ==> { r.from must beSome }) }

  `from` is always the smallest `froms`:

    ${ prop((r: Range[FeatureId]) => r.from.forall(d => r.froms.forall(_ >= d))) }

  `from` is always none when `froms` is empty:

    ${ prop((id: FeatureId, to: Date) => Range(id, Nil, to).from must beNone) }

  `fromOrMin` is always min when `froms` is empty:

    ${ prop((id: FeatureId, to: Date) => Range(id, Nil, to).fromOrMin ==== Date.minValue) }

  `fromOrMax` is always max when `froms` is empty:

    ${ prop((id: FeatureId, to: Date) => Range(id, Nil, to).fromOrMax ==== Date.maxValue) }

"""
}
