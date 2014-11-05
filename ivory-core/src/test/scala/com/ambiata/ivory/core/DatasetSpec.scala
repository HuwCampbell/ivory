package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class DatasetSpec extends Specification with ScalaCheck { def is = s2"""

Combinators
-----------

  FactsetDataset fold only evaluates 'factset' expression:

    ${ prop((n: Int, factset: Factset) =>
         FactsetDataset(factset).fold(_ -> n, _ => ???) ==== (factset -> n)) }


  SnapshotDataset fold only evaluates 'snapshot' expression:

    ${ prop((n: Int, snapshot: Snapshot) =>
         SnapshotDataset(snapshot).fold(_ => ???, _ -> n) ==== (snapshot -> n)) }

  Fold constructors is identity:

    ${ prop((dataset: Dataset) =>
         dataset.fold[Dataset](FactsetDataset, SnapshotDataset) ==== dataset) }

  isFactset is true iff it is indeed a factset:

    ${ prop((f: Factset) => FactsetDataset(f).isFactset ==== true) }
    ${ prop((s: Snapshot) => SnapshotDataset(s).isFactset ==== false) }

  isSnapshot is true iff it is indeed a snapshot:

    ${ prop((f: Factset) => FactsetDataset(f).isSnapshot ==== false) }
    ${ prop((s: Snapshot) => SnapshotDataset(s).isSnapshot ==== true) }

  isFactset and isSnapshot are mutually exclusive:

    ${ prop((dataset: Dataset) => dataset.isFactset ^ dataset.isSnapshot) }

  isEmpty is dependent on the partitions for factsets:

    ${ prop((f: Factset) => FactsetDataset(f).isEmpty ==== f.partitions.isEmpty) }

  isEmpty is always false for snapshots:

    ${ prop((s: Snapshot) => SnapshotDataset(s).isEmpty ==== false) }

Constructors
------------

  Convenience (for inference) are just aliases with better types:

    ${ prop((f: Factset) => Dataset.factset(f) ==== FactsetDataset(f)) }
    ${ prop((s: Snapshot) => Dataset.snapshot(s) ==== SnapshotDataset(s)) }

  A snapshot always has the lowest priority:

    ${ prop((s: Snapshot) => Dataset.prioritizedSnapshot(s).priority ==== Priority.Max) }

  Creating datasets from a features store respects exclusive lower bound and inclusive
  upper bounds:

    ${ prop((s: FeatureStore, dates: UniqueDates) =>
      Dataset.within(s, dates.earlier, dates.later).forall({
        case Prioritized(_, FactsetDataset(f)) =>
          f.partitions.forall(p => p.date > dates.earlier && p.date <= dates.later)
        case Prioritized(_, SnapshotDataset(f)) =>
          false // should not be any snapshots
      })) }

    ${ prop((s: FeatureStore, date: Date) =>
      Dataset.to(s, date).forall({
        case Prioritized(_, FactsetDataset(f)) =>
          f.partitions.forall(p => p.date <= date)
        case Prioritized(_, SnapshotDataset(f)) =>
          false // should not be any snapshots
      })) }


  Creating datasets from a features store keeps all data within bound:

    ${ prop((s: FeatureStore, dates: UniqueDates) =>
      Dataset.within(s, dates.earlier, dates.later).map({
        case Prioritized(_, FactsetDataset(f)) =>
          f.partitions.length
        case Prioritized(_, SnapshotDataset(f)) =>
          0
      }).sum ==== s.filterByDate(date => date > dates.earlier && date <= dates.later).factsets.map(_.value.partitions.size).sum ) }

    ${ prop((s: FeatureStore, date: Date) =>
      Dataset.to(s, date).map({
        case Prioritized(_, FactsetDataset(f)) =>
          f.partitions.length
        case Prioritized(_, SnapshotDataset(f)) =>
          0
      }).sum ==== s.filterByDate(_ <= date).factsets.map(_.value.partitions.size).sum ) }

"""
}
