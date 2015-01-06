package com.ambiata.ivory.core

import arbitraries.Arbitraries._
import org.specs2._
import scalaz.scalacheck.ScalazProperties._

class DictionarySpec extends Specification with ScalaCheck { def is = s2"""

Laws
----
  Equal                                        ${equal.laws[Dictionary]}

Dictionary Tests
----------------

Indexing:
  By feature id contains matching entries                        $byFeatureId
  By feature index contains matching entries                     $byFeatureIndex
  By feature index (reverse) contains matching entries           $byFeatureIndexReverse
  By feature index matches reverse index                         $symmetricalFeatureIndex
  By concrete contains all features                              $byConcrete
  By concrete is lossless                                        $byConcreteLossless

Indexing count cross checks:
  By feature id contains same count as dictionary                $byFeatureIdCount
  By feature index contains same count as dictionary             $byFeatureIndexCount
  By feature index (reverse) contains same count as dictionary   $byFeatureIndexReverseCount
  By concrete index contains same count as dictionary            $byConcreteCount
  By concrete feature index (reverse) is the same as dictionary  $byConcreteFeatureIndexReverse

Filtering:
  By namespace leaves only definitions for that namespace        $filterNamespace
  By a feature leaves only definitions matching those features   $filterFeature

Size:
  Always matches number of definitions (just an alias)           $size

Append:
  Appending dictionaries is just appending there definitions     $append

Exists:
  Checking the existence of virtual features                     $hasVirtual

Windows:
  Same number of window entries as concrete features             $windowCount
  Windows match feature windows                                  $window

"""


  def byFeatureId = propNoShrink((dictionary: Dictionary) =>
    seqToResult(dictionary.definitions.map(d =>
      dictionary.byFeatureId.get(d.featureId) must beSome(d))))

  def byFeatureIndex = propNoShrink((dictionary: Dictionary) =>
    seqToResult((1 until dictionary.definitions.size).toList.map(n =>
      dictionary.byFeatureIndex.get(n) must beSome)))

  def byFeatureIndexReverse = propNoShrink((dictionary: Dictionary) =>
    seqToResult(dictionary.definitions.map(d =>
      dictionary.byFeatureIndexReverse.get(d.featureId) must beSome)))

  def symmetricalFeatureIndex = propNoShrink((dictionary: Dictionary) =>
    seqToResult(dictionary.definitions.map(d =>
      dictionary.byFeatureIndexReverse.get(d.featureId).flatMap(n =>
       dictionary.byFeatureIndex.get(n)) must beSome(d))))

  def byConcrete = propNoShrink((dictionary: Dictionary) =>
    dictionary.byConcrete.sources.flatMap(f => f._1 :: f._2.virtual.map(_._1)).toSet ==== dictionary.byFeatureId.keySet
  )

  def byConcreteLossless = propNoShrink((dictionary: Dictionary) =>
    dictionary.byConcrete.dictionary.byFeatureId ==== dictionary.byFeatureId
  )

  def byFeatureIdCount = propNoShrink((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.byFeatureId.size)

  def byFeatureIndexCount = propNoShrink((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.byFeatureIndex.size)

  def byFeatureIndexReverseCount = propNoShrink((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.byFeatureIndexReverse.size)

  def byConcreteCount = propNoShrink((dictionary: Dictionary) =>
    dictionary.byConcrete.sources.map(1 + _._2.virtual.size).sum ==== dictionary.size
  )

  def byConcreteFeatureIndexReverse = propNoShrink { (dictionary: Dictionary) =>
    val conc = dictionary.byConcrete
    conc.byFeatureIndexReverse ==== dictionary.forFeatureIds(conc.sources.keySet).byFeatureIndexReverse
  }

  def filterNamespace = propNoShrink((dictionary: Dictionary, n: Int) => (n > 0 && dictionary.size > 0) ==> {
    val namespace = dictionary.definitions(n % dictionary.size).featureId.namespace
    val filtered = dictionary.forNamespace(namespace)
    filtered.definitions.forall(_.featureId.namespace == namespace) && filtered.definitions.size >= 1
  })

  def filterFeature = propNoShrink((dictionary: Dictionary, n: Int) => (n > 0 && dictionary.size > 0) ==> {
    val featureId = dictionary.definitions(n % dictionary.size).featureId
    val filtered = dictionary.forFeatureIds(Set(featureId))
    filtered.definitions.forall(_.featureId == featureId) && filtered.definitions.size == 1
  })

  def size = propNoShrink((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.size)

  def append = propNoShrink((d1: Dictionary, d2: Dictionary) =>
    d1.append(d2) must_== Dictionary(d1.definitions ++ d2.definitions))

  def hasVirtual = propNoShrink((d1: Dictionary) =>
    d1.hasVirtual ==== d1.byConcrete.sources.values.exists(_.virtual.nonEmpty))

  def windowCount = prop((d: Dictionary) =>
    d.windows.features.size ==== d.byConcrete.sources.size)

  def window = prop((d: Dictionary) =>
    d.windows.features.flatMap(_.windows).toSet ==== d.definitions.flatMap({
      case Concrete(id, definition) =>
        Nil
      case Virtual(id, definition) =>
        definition.window.toList
    }).toSet)
}
