package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import org.specs2.matcher.MustThrownMatchers

// Please do _not_ use this trait - it's just for legacy tests. Learn you a property test for Great Good.
trait SampleFacts extends MustThrownMatchers {

  val sampleDictionary = Dictionary(List(
    Definition.concrete(FeatureId(Namespace("ns1"), "fid1"), StringEncoding, Mode.State, Some(CategoricalType), "desc", Nil),
    Definition.concrete(FeatureId(Namespace("ns1"), "fid2"), IntEncoding, Mode.State, Some(NumericalType), "desc", Nil),
    Definition.concrete(FeatureId(Namespace("ns2"), "fid3"), BooleanEncoding, Mode.State, Some(CategoricalType), "desc", Nil)
  ))

  val sampleFacts: List[List[Fact]] = List(List(
    StringFact("eid1",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
    StringFact("eid1",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 9, 1), Time(0), "def"),
    IntFact("eid2",     FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
    IntFact("eid2",     FeatureId(Namespace("ns1"), "fid2"), Date(2012, 11, 1), Time(0), 11),
    BooleanFact("eid3", FeatureId(Namespace("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true)
  ), List(
    StringFact("eid1", FeatureId(Namespace("ns1"), "fid1"), Date(2012, 9, 1), Time(0), "ghi")
  ))

}
