package com.ambiata.ivory.core

import com.ambiata.mundane.io.MemoryConversions
import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._

import scalaz._, Scalaz._
import MemoryConversions._

class SkewSpec extends Specification with ScalaCheck { def is = s2"""

Skew Tests
----------

 If each namespace has a smaller size than the optimal size per reducer, 
 we allocate one reducer per namespace $small

 If a namespace has a larger size than optimal _and_ there is more than one feature in that namespace, that
 namespace should be allocated to more than one reducer $spread

 If a namespace is larger than features * optimal, every feature should be in its assigned its own
 reducer $large

"""
   def small = {
     val namespaces = List(
       "demographics"   -> 1.mb
       , "offers"       -> 1.mb
       , "widgets"      -> 1.mb
       , "knobs"        -> 1.mb
       , "flavours"     -> 1.mb
     )
     val (reducersNb, _) = Skew.calculate(dictionary, namespaces, optimalSize)
     reducersNb must_== namespaces.size
   }

  def spread = {
    val (_, r) = Skew.calculate(dictionary, largeNamespace, optimalSize)
    
    largeNamespace.forall { case (name, size) =>
      val all = r.filter(_._1 == name)
      val reducers = all.map(_._3).distinct.size
      if (size > optimalSize && dictionary.forNamespace(name).meta.size > 1)
        reducers > 1
      else
        reducers == 1
    }
  }

  def large = {
    val (_, r) = Skew.calculate(dictionary, largeNamespace, optimalSize)
    largeNamespace.filter { case (n, size) =>
      size.toBytes.value > dictionary.forNamespace(n).meta.size.toLong * optimalSize.toBytes.value
    }.forall { case (n, size) =>
      val all = r.filter(_._1 == n)
      all.map(_._3).distinct.size must_== all.size
    }
  }

  def largeNamespace = List(
    "demographics" -> 25986865.bytes
  , "offers"       -> 57890389.bytes
  , "widgets"      -> 329028927.bytes
  , "knobs"        -> 8380852917L.bytes
  , "flavours"     -> 184072795.bytes
  )

  def fake = FeatureMeta(DoubleEncoding, Some(ContinuousType), "desc", Nil)
  def optimalSize = 256.mb

  /** create a dictionary */
  def dictionary = Dictionary(featureIds.map(_ -> fake).toMap)
  def featureIds =
    (1 to 10).map(n => FeatureId("demographics", "d" + n)).toList ++
    (1 to 10).map(n => FeatureId("offers", "o" + n)).toList ++
    (1 to 10).map(n => FeatureId("widgets", "w" + n)).toList ++
    (1 to 10).map(n => FeatureId("knobs", "k" + n)).toList ++
    (1 to 10).map(n => FeatureId("flavours", "f" + n)).toList
  
}
