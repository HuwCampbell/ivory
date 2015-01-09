package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class DatasetsSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----
  Equal                                        ${equal.laws[Datasets]}

Combinators
-----------

  byPriority returns the datasets in sorted order:

    ${ prop((a: Datasets) =>
         a.byPriority.sets ==== a.sets.sorted) }

  concat just joins the two datasets ignoring priority:

    ${ prop((a: Datasets, b: Datasets) =>
         (a ++ b) ==== Datasets(a.sets ++ b.sets)) }

  `+:` add a prioritized dataset:

    ${ prop((ds: Datasets, d: Prioritized[Dataset]) =>
         (d +: ds).sets.length ==== (ds.sets.length + 1)) }

    ${ prop((ds: Datasets, d: Prioritized[Dataset]) =>
         (d +: ds).sets.contains(d)) }

  add is just an alias for `+:`

    ${ prop((ds: Datasets, d: Prioritized[Dataset]) =>
         ds.add(d.priority, d.value) ==== (d +: ds)) }

  filter const true is identity:

    ${ prop((a: Datasets) => a.filter(_ => true) ==== a) }

  filter const false is the empty Datasets:

    ${ prop((a: Datasets) =>  a.filter(_ => false) ==== Datasets.empty) }

  filter on something we know exists never results in empty Datasets and
  contains the thing we filter on:

    ${ prop((a: Datasets, d: Prioritized[Dataset]) =>
         (d +: a).filter(_ === d.value) !=== Datasets.empty) }

    ${ prop((a: Datasets, d: Prioritized[Dataset]) =>
         (d +: a).filter(_ === d.value).sets.contains(d)) }

  prune leaves no empty datasets:

    ${ prop((ds: Datasets, p: Priority, f: FactsetId) =>
         ds.add(p, FactsetDataset(Factset(f, FactsetFormat.V2, Nil))).prune.sets.forall(!_.value.isEmpty)) }

Constructors
------------

  Empty is suprisingly enough just the empty list:

    ${ Datasets.empty.sets ==== Nil }

"""
}
