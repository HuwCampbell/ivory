package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.{FeatureIdLookup, FactsetLookup, FactsetVersionLookup}
import com.ambiata.ivory.storage.fact._

object FactsetLookups {
  def priorityTable(globs: List[Prioritized[FactsetGlob]]): FactsetLookup = {
    val lookup = new FactsetLookup
    globs.foreach(p => lookup.putToPriorities(p.value.factset.render, p.priority.toShort))
    lookup
  }

  def versionTable(globs: List[FactsetGlob]): FactsetVersionLookup = {
    val lookup = new FactsetVersionLookup
    globs.foreach(g => lookup.putToVersions(g.factset.render, g.version.toByte))
    lookup
  }
}
