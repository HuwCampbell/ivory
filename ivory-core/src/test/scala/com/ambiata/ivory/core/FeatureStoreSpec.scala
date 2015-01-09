package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import scalaz.scalacheck.ScalazProperties._

class FeatureStoreSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----
  Equal                                        ${equal.laws[FeatureStore]}


Combinators
-----------

  Conveniences only, just ensuring consistency:

    ${ prop((s: FeatureStore) => s.unprioritizedIds ==== s.factsets.map(_.value.id)) }

    ${ prop((s: FeatureStore) => s.factsetIds ==== s.factsets.map(_.map(_.id))) }

    ${ prop((s: FeatureStore) => s.unprioritizedIds ==== s.factsetIds.map(_.value)) }

    ${ prop((s: FeatureStore) => s.toDataset.size ==== s.factsets.size) }

    ${ prop((s: FeatureStore) => s.toDataset.forall(_.value.isFactset)) }


  Basic rules for filtering, i.e. const true is identity, const false is empty:

    ${ prop((s: FeatureStore) => s.filterByPartition(_ => true) ==== s) }

    ${ prop((s: FeatureStore) => s.filterByDate(_ => true) ==== s) }

    ${ prop((s: FeatureStore) => s.filterByFactsetId(_ => true) ==== s) }

    ${ prop((s: FeatureStore) => s.filter(_ => true) ==== s) }

    ${ prop((s: FeatureStore) => s.filterByPartition(_ => false).factsets.forall(_.value.partitions.isEmpty)) }

    ${ prop((s: FeatureStore) => s.filterByDate(_ => false).factsets.forall(_.value.partitions.isEmpty)) }

    ${ prop((s: FeatureStore) => s.filterByFactsetId(_ => false).factsets.forall(_.value.partitions.isEmpty)) }

    ${ prop((s: FeatureStore) => s.filter(_ => false).factsets.forall(_.value.partitions.isEmpty)) }


  `diff` means that the entries in the resultant feature store must only exist in one of the sources:

    ${ prop((s: FeatureStore, t: FeatureStore) =>
         s.diff(t).factsets.length <= (s.factsets.length + t.factsets.length)) }

    ${ prop((s: FeatureStore, t: FeatureStore) =>
         s.diff(t).factsets.forall(x => s.factsets.map(_.value).contains(x.value) ^
                                        t.factsets.map(_.value).contains(x.value))) }

  `subsetOf` is always true for the empty FeatureStore:

    ${ prop((id: FeatureStoreId, s: FeatureStore) =>
         FeatureStore(id, Nil).subsetOf(s) ==== true) }

  `subsetOf` means that all factsets in 'this' store must appear in 'other' feature store:

    ${ prop((s: FeatureStore, t: FeatureStore) =>
         s.subsetOf(t) ==== s.factsets.forall(f => t.factsets.exists(_.value.id == f.value.id))) }

"""
}
