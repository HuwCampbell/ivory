package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class FactsetLookupsSpec extends Specification with ScalaCheck { def is = s2"""

Properties
----------

  FactsetLookup has the same number and value of entries as factsets in the datasets.

    ${ prop((d: Datasets) =>
         FactsetLookups.priorityTable(d).getPriorities.size ====
           d.sets.filter(_.value.isFactset).size ) }

    ${ prop((d: Datasets) =>
         FactsetLookups.priorityTable(d).getPriorities.asScala.mapValues(_.toShort).toSet ====
           d.sets.flatMap(dd => dd.value.toFactset.toList.map(f => (f.id.render, dd.priority.toShort))).toSet) }


  FactsetVersionLookup has the same number and value of entries as factsets in the datasets.

    ${ prop((d: Datasets) =>
         FactsetLookups.versionTable(d).getVersions.size ====
           d.sets.filter(_.value.isFactset).size) }

    ${ prop((d: Datasets) =>
         FactsetLookups.versionTable(d).getVersions.asScala.mapValues(_.toByte).toSet ====
           d.sets.flatMap(dd => dd.value.toFactset.toList.map(f => (f.id.render, f.format.toByte))).toSet) }

"""
}
