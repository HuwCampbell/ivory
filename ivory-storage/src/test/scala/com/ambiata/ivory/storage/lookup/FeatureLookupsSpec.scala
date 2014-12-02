package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class FeatureLookupsSpec extends Specification with ScalaCheck { def is = s2"""

isSetTable
----------

  There should be a flag entry for each concrete feature.

    $concretes

  The number of true flags should be the same as the number of
  entries that are sets.

    $sets

  There should be a flag entry for each concrete feature.

    $isSetTableConcrete


featureIdTable
--------------

  Every feature definition gets an entry in the lookup (note,
  that this behaviour may change in the future, but for now some
  lookups remain sparse when used for concrete only feature in
  order to use a consistent feature indexing scheme throughout).

    $size

  Feature ids should correspond to what is on the dictionary
  index.

    $index

isSetLookupToArray
------------------

  Counts of true and false should match lookup.

    $flags

spareMapToArray
---------------

  Convert a sparse map of values to an array with defaults.

    $sparseMapToArray
"""

  def concretes = prop((d: Dictionary) =>
   FeatureLookups.isSetTable(d).getFlags.size ====
     d.byConcrete.sources.size)

  def sets = prop((d: Dictionary) =>
   FeatureLookups.isSetTable(d).getFlags.asScala.filter(_._2).size ====
     d.definitions.filter({
       case Concrete(_, definition) =>
         definition.mode.isSet
       case Virtual(_, _) =>
         false
     }).size)

  def isSetTableConcrete = prop((d: Dictionary) =>
    FeatureLookups.isSetTableConcrete(d.byConcrete).getFlags.size() ==== (d.byConcrete.byFeatureIndexReverse.values.max + 1)
  )

  def size = prop((d: Dictionary) =>
    FeatureLookups.featureIdTable(d).getIds.size ====
      d.definitions.size)

  def index = prop((d: Dictionary) =>
    FeatureLookups.featureIdTable(d).getIds.asScala.toMap ====
      d.byFeatureIndex.map({  case (n, m) => m.featureId.toString -> Integer.valueOf(n) }))

  def flags = prop((d: Dictionary) => {
    val table = FeatureLookups.isSetTable(d)
    val array = FeatureLookups.isSetLookupToArray(table)
    (table.getFlags.asScala.filter(_._2).size ==== array.filter(identity).length) })

  // Using Byte just to speed up the test - otherwise we create some _really_ big arrays
  def sparseMapToArray = prop((ls: List[(Byte, String)]) => ls.nonEmpty ==> {
    val l = ls.toMap.map { case (i, s) => Math.abs(i) -> s }
    // Using null here as a sentinel value that we know ScalaCheck won't generate for us
    val a = FeatureLookups.sparseMapToArray(l.toList, null)
    (a.length, a.toList.filterNot(_ == null)) ==== ((l.maxBy(_._1)._1 + 1, l.toList.sortBy(_._1).map(_._2)))
  })

}
