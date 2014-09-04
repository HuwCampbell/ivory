package com.ambiata.ivory.core

import org.specs2._

class DictionarySpec extends Specification with ScalaCheck { def is = s2"""

Dictionary Tests
----------------

Indexing:
  By feature id contains matching entries                        $byFeatureId
  By feature index contains matching entries                     $byFeatureIndex
  By feature index (reverse) contains matching entries           $byFeatureIndexReverse
  By feature index matches reverse index                         $symmetricalFeatureIndex
  By concrete contains all features                              $byConcrete

Indexing count cross checks:
  By feature id contains same count as dictionary                $byFeatureIdCount
  By feature index contains same count as dictionary             $byFeatureIndexCount
  By feature index (reverse) contains same count as dictionary   $byFeatureIndexReverseCount
  By concrete index contains same count as dictionary            $byConcreteCount

Filtering:
  By namespace leaves only definitions for that namespace        $filterNamespace
  By a feature leaves only definitions matching those features   $filterFeature

Size:
  Always matches number of definitions (just an alias)           $size

Append:
  Appending dictionaries is just appending there definitions     $append

"""
  import Arbitraries._

  def byFeatureId = prop((dictionary: Dictionary) =>
    seqToResult(dictionary.definitions.map(d =>
      dictionary.byFeatureId.get(d.featureId) must beSome(d))))

  def byFeatureIndex = prop((dictionary: Dictionary) =>
    seqToResult((1 until dictionary.definitions.size).toList.map(n =>
      dictionary.byFeatureIndex.get(n) must beSome)))

  def byFeatureIndexReverse = prop((dictionary: Dictionary) =>
    seqToResult(dictionary.definitions.map(d =>
      dictionary.byFeatureIndexReverse.get(d) must beSome)))

  def symmetricalFeatureIndex = prop((dictionary: Dictionary) =>
    seqToResult(dictionary.definitions.map(d =>
      dictionary.byFeatureIndexReverse.get(d).flatMap(n =>
       dictionary.byFeatureIndex.get(n)) must beSome(d))))

  def byConcrete = prop((dictionary: Dictionary) =>
    dictionary.byConcrete.sources.flatMap(f => f._1 :: f._2.virtual.map(_._1)).toSet ==== dictionary.byFeatureId.keySet
  )

  def byFeatureIdCount = prop((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.byFeatureId.size)

  def byFeatureIndexCount = prop((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.byFeatureIndex.size)

  def byFeatureIndexReverseCount = prop((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.byFeatureIndexReverse.size)

  def byConcreteCount = prop((dictionary: Dictionary) =>
    dictionary.byConcrete.sources.map(1 + _._2.virtual.size).sum ==== dictionary.size
  )

  def filterNamespace = prop((dictionary: Dictionary, n: Int) => (n > 0 && dictionary.size > 0) ==> {
    val namespace = dictionary.definitions(n % dictionary.size).featureId.namespace
    val filtered = dictionary.forNamespace(namespace)
    filtered.definitions.forall(_.featureId.namespace == namespace) && filtered.definitions.size >= 1
  })

  def filterFeature = prop((dictionary: Dictionary, n: Int) => (n > 0 && dictionary.size > 0) ==> {
    val featureId = dictionary.definitions(n % dictionary.size).featureId
    val filtered = dictionary.forFeatureIds(Set(featureId))
    filtered.definitions.forall(_.featureId == featureId) && filtered.definitions.size == 1
  })

  def size = prop((dictionary: Dictionary) =>
    dictionary.definitions.size must_== dictionary.size)

  def append = prop((d1: Dictionary, d2: Dictionary) =>
    d1.append(d2) must_== Dictionary(d1.definitions ++ d2.definitions))
}
