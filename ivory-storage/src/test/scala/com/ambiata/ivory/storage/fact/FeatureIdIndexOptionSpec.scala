package com.ambiata.ivory.storage.fact

import com.ambiata.disorder._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.lookup.FeatureIdLookup
import org.specs2._

class FeatureIdIndexOptionSpec extends Specification with ScalaCheck { def is = s2"""

  Empty is always empty
    $empty

  Natural numbers are always defined
    $defined

  Natural numbers return a valid id
    $get

  Empty will throw error on get
    $getError

  Negative value with throw error
    $fromIntError

  Lookup fact when exists
    $lookupExists

  Lookup fact when empty
    $lookupEmpty
"""

  def empty =
    FeatureIdIndexOption.empty.isEmpty

  def defined = prop((n: NaturalInt) =>
    FeatureIdIndexOption.fromInt(n.value).isDefined
  )

  def get = prop((n: NaturalInt) =>
    FeatureIdIndexOption.fromInt(n.value).get.int ==== n.value
  )

  def getError =
    FeatureIdIndexOption.empty.get must throwA[RuntimeException]

  def fromIntError = prop((n: NaturalInt) =>
    FeatureIdIndexOption.fromInt(n.value * -1) must throwA[RuntimeException]
  )

  def lookupExists = prop((f: Fact, l: List10[Fact]) =>
    FeatureIdIndexOption.lookup(f, lookupFromFacts(l.value :+ f)).get.int ==== l.value.length
  )

  def lookupEmpty = prop((f: Fact, l: List10[Fact]) =>
    FeatureIdIndexOption.lookup(f, lookupFromFacts(l.value)).isEmpty
  )

  def lookupFromFacts(l: List[Fact]): FeatureIdLookup = {
    val lookup = new FeatureIdLookup(new java.util.HashMap[String, Integer])
    l.zipWithIndex.foreach(f => lookup.putToIds(f._1.featureId.toString, f._2))
    lookup
  }
}
