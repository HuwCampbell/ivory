package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.gen._

import org.specs2._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._
import scalaz.scalacheck.ScalaCheckBinding._


class RangesSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Equal                                        ${equal.laws[Ranges[FeatureId]]}

Properties
----------

  If `containedBy` is true, this implies that all ids in `a` are also in `b`:

    ${ prop((contained: ContainedRanges) =>
         (contained.base.values.map(_.id).toSet -- contained.containedBy.values.map(_.id).toSet) ==== Set() ) }

  If `containedBy` is true, this implies that the minimum from of `b` is less than or equal to the minimum from of `a`.

    ${ prop((contained: ContainedRanges) =>
         contained.base.values.forall(r => contained.containedBy.values.find(_.id == r.id).exists(_.fromOrMin <= r.fromOrMin))) }

  If `containedBy` is not true, then either a id from base is not in containted, or a date does not overlap.

    ${ prop((base: Ranges[FeatureId], contained: Ranges[FeatureId]) => !base.containedBy(contained) ==> {
         !(base.values.map(_.id).toSet -- contained.values.map(_.id).toSet).isEmpty ||
           base.values.exists(r => !contained.values.find(_.id == r.id).exists(_.fromOrMin <= r.fromOrMin)) }) }

  Sanity check on arbitrary.

    ${ prop((contained: ContainedRanges) => contained.base.containedBy(contained.containedBy)) }


Namespace Ranges
----------------

  Number of distinct namespaces, to pairs should be the number of entries in the output:

    ${ prop((f: Ranges[FeatureId]) =>
         Ranges.toNamespaces(f).values.size ==== f.values.map(r => r.id.namespace -> r.to).toSet.size) }

  Distinct froms in input should be maintained in output:

    ${ prop((f: Ranges[FeatureId]) =>
         Ranges.toNamespaces(f).values.flatMap(_.froms).toSet ==== f.values.flatMap(_.froms).toSet) }

  Number of froms in input should be maintained in output:

    ${ prop((f: Ranges[FeatureId]) =>
         Ranges.toNamespaces(f).values.flatMap(_.froms).size ==== f.values.flatMap(_.froms).size) }

"""

  case class ContainedRanges(base: Ranges[FeatureId], containedBy: Ranges[FeatureId])

  implicit def ContainedRangesArbitrary: Arbitrary[ContainedRanges] =
    Arbitrary(for {
      base <- arbitrary[Ranges[FeatureId]]
      fresh <- arbitrary[List[FeatureId]].map(_.filter(f => !base.values.exists(_.id === f)))
      extra <- fresh.traverse(GenDictionary.rangeOf)
      warped <- base.values.traverse(r => for {
        days <- Gen.choose(0, 100)
        froms = r.froms.map(DateTimeUtil.minusDays(_, days))
      } yield Range(r.id, froms, r.to))
      containedBy = base.copy(values = warped ++ extra)
    } yield ContainedRanges(base, containedBy))
}
