package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.storage.arbitraries.DictionaryWithoutKeyedSet
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class FeatureLookupsSpec extends Specification with ScalaCheck { def is = s2"""

isSetTable
----------

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

  def isSetTableConcrete = prop((d: DictionaryWithoutKeyedSet) =>
    FeatureLookups.isSetTableConcrete(d.value.byConcrete).getFlags.size() ====
      (d.value.byConcrete.byFeatureIndexReverse.values.max + 1)
  )

  def size = prop((d: DictionaryWithoutKeyedSet) =>
    FeatureLookups.featureIdTable(d.value).getIds.size ====
      d.value.definitions.size)

  def index = prop((d: DictionaryWithoutKeyedSet) =>
    FeatureLookups.featureIdTable(d.value).getIds.asScala.toMap ====
      d.value.byFeatureIndex.map({  case (n, m) => m.featureId.toString -> Integer.valueOf(n) }))

  def flags = prop((d: DictionaryWithoutKeyedSet) => {
    val table = FeatureLookups.isSetTableConcrete(d.value.byConcrete)
    val array = FeatureLookups.isSetLookupToArray(table)
    table.getFlags.asScala.count(_._2) ==== array.filter(identity).length })

  // Using Byte just to speed up the test - otherwise we create some _really_ big arrays
  def sparseMapToArray = prop((ls: List[(Byte, String)]) => ls.nonEmpty ==> {
    val l = ls.toMap.map { case (i, s) => Math.abs(i) -> s }
    // Using null here as a sentinel value that we know ScalaCheck won't generate for us
    val a = FeatureLookups.sparseMapToArray(l.toList, null)
    (a.length, a.toList.filterNot(_ == null)) ==== ((l.maxBy(_._1)._1 + 1, l.toList.sortBy(_._1).map(_._2)))
  })

}
