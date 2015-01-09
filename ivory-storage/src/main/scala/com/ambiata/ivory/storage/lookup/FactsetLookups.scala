package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.{FeatureIdLookup, FactsetLookup, FactsetVersionLookup}

object FactsetLookups {
  def priorityTable(datasets: Datasets): FactsetLookup = {
    val lookup = new FactsetLookup(new java.util.HashMap[String, java.lang.Short])
    datasets.sets.foreach(p => p.value match {
      case FactsetDataset(factset) =>
        lookup.putToPriorities(factset.id.render, p.priority.toShort)
      case SnapshotDataset(_) =>
        ()
    })
    lookup
  }

  def versionTable(datasets: Datasets): FactsetVersionLookup = {
    val lookup = new FactsetVersionLookup(new java.util.HashMap[String, java.lang.Byte])
    datasets.sets.foreach(p => p.value match {
      case FactsetDataset(factset) =>
        lookup.putToVersions(factset.id.render, factset.format.toByte)
      case SnapshotDataset(_) =>
        ()
    })
    lookup
  }
}
