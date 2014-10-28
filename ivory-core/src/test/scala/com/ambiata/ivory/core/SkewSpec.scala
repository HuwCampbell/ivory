package com.ambiata.ivory.core

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.mundane.io.BytesQuantity
import com.ambiata.mundane.io.MemoryConversions._
import org.specs2._
import org.specs2.execute.Result

import scalaz._, Scalaz._, scalaz.scalacheck.ScalazArbitrary._

class SkewSpec extends Specification with ScalaCheck { def is = s2"""

Skew Tests
----------

 If each namespace has a smaller size than the optimal size per reducer,
 we allocate one reducer per namespace $small

 If a namespace has a larger size than optimal _and_ there is more than one feature in that namespace, that
 namespace should be allocated to more than one reducer $spread

 If one namespace has most of the data in a small number of features
 then it should have a greater count (eg. structs)      $largeFeature

 If a namespace is larger than features * optimal, every feature should be in its assigned its own
 reducer $large

 Property test for arbitrary features                   $allFeatures

"""
   def small = {
     val namespaces = List(
         Name("demographics") -> 1.mb
       , Name("offers"      ) -> 1.mb
       , Name("widgets"     ) -> 1.mb
       , Name("knobs"       ) -> 1.mb
       , Name("flavours"    ) -> 1.mb
     )
     val (reducersNb, _) = Skew.calculate(dictionary, namespaces, optimalSize)
     reducersNb must_== namespaces.size
   }

  def spread =
    checkAll(dictionary, largeNamespace)

  def largeFeature =
    checkAll(Dictionary(featureIdsWithLargeFeature.map(fake.toDefinition)), largeNamespace)

  def checkSpread(dict: Dictionary, namespace: List[(Name, BytesQuantity)]): Result = {
    val (nb, r) = Skew.calculate(dict, namespace, optimalSize)
    seqToResult(namespace.map { case (name, size) =>
      val all = r.filter(_._1 == name)
      val reducers = all.groupBy(_._3.offset).values.map(_.map(_._3.count).max).sum.toInt
      if (size > optimalSize)
        reducers must beGreaterThan(1)
      else
        reducers ==== 1
    })
  }

  def large = {
    val (_, r) = Skew.calculate(dictionary, largeNamespace, optimalSize)
    largeNamespace.filter { case (n, size) =>
      size.toBytes.value > dictionary.forNamespace(n).definitions.size.toLong * optimalSize.toBytes.value
    }.forall { case (n, size) =>
      val all = r.filter(_._1 == n)
      all.map(_._3.offset).distinct.size must_== all.size
    }
  }

  def allFeatures = prop { (features: NonEmptyList[(ConcreteGroupFeature, Int)]) =>
    val dict = features.list.foldLeft(Dictionary.empty)(_ append _._1.dictionary)
    val namespace = features.list.map { case (cgf, size) => cgf.fid.namespace -> (size & (Int.MaxValue - 1) + 1).bytes }
    checkAll(dict, namespace)
  }

  def checkAll(dict: Dictionary, namespace: List[(Name, BytesQuantity)]): Result =
    checkSpread(dict, namespace) and everyReducerBeingUsed(dict, namespace)

  /** Calculate whether given the number of reducers, at _least_ one feature is utilising it */
  def everyReducerBeingUsed(dict: Dictionary, namespace: List[(Name, BytesQuantity)]): Result = {
    val (nb, r) = Skew.calculate(dict, namespace, optimalSize)
    val a = new Array[Boolean](nb)
    r.foreach {
      case (ns, fid, o) => (o.offset until o.offset + o.count).foreach {
        i => a(i) = true
      }
    }
    a ==== Array.fill(nb)(true)
  }

  def largeNamespace = List(
    Name("demographics") -> 25986865.bytes
  , Name("offers"      ) -> 57890389.bytes
  , Name("widgets"     ) -> 329028927.bytes
  , Name("knobs"       ) -> 8380852917L.bytes
  , Name("flavours"    ) -> 184072795.bytes
  )

  def fake = ConcreteDefinition(DoubleEncoding, Mode.State, Some(ContinuousType), "desc", Nil)
  def optimalSize = 256.mb

  /** create a dictionary */
  def dictionary = Dictionary(featureIds.map(fake.toDefinition))
  def featureIds =
    (1 to 10).map(n => FeatureId(Name("demographics"), "d" + n)).toList ++
    (1 to 10).map(n => FeatureId(Name("offers"), "o" + n)).toList ++
    (1 to 10).map(n => FeatureId(Name("widgets"), "w" + n)).toList ++
    (1 to 10).map(n => FeatureId(Name("knobs"), "k" + n)).toList ++
    (1 to 10).map(n => FeatureId(Name("flavours"), "f" + n)).toList

  def featureIdsWithLargeFeature =
    (1 to 10).map(n => FeatureId(Name("demographics"), "d" + n)).toList ++
      (1 to 10).map(n => FeatureId(Name("offers"), "o" + n)).toList ++
      (1 to 10).map(n => FeatureId(Name("widgets"), "w" + n)).toList ++
      (1 to 1).map(n => FeatureId(Name("knobs"), "k" + n)).toList ++
      (1 to 10).map(n => FeatureId(Name("flavours"), "f" + n)).toList
}
