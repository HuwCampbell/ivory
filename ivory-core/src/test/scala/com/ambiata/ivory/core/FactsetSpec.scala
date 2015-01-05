package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import scalaz.scalacheck.ScalazProperties._

class FactsetSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----
  Equal                                        ${equal.laws[Factset]}

Combinators
-----------

  `filterByPartition` is just a convenience on the underlying list of partitions.

    ${ prop((f: Factset, p: Partition) =>
         f.filterByPartition(_ == p).partitions ==== f.partitions.filter(_.value == p)) }

  `filterByDate` is just a convenience on the underlying list of partition dates.

    ${ prop((f: Factset, d: Date) =>
         f.filterByDate(_ > d).partitions ==== f.partitions.filter(_.value.date > d)) }

  Basic rules for filtering, i.e. const true is identity, const false is empty:

    ${ prop((f: Factset) => f.filterByPartition(_ => true) ==== f) }

    ${ prop((f: Factset) => f.filterByDate(_ => true) ==== f) }

    ${ prop((f: Factset) => f.filterByPartition(_ => false).partitions ==== Nil) }

    ${ prop((f: Factset) => f.filterByDate(_ => false).partitions ==== Nil) }

"""
}
