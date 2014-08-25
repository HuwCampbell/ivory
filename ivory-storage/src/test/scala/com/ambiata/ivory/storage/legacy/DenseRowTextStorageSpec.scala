package com.ambiata.ivory.storage.legacy

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles

import org.specs2._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._, FactFormats._
import com.ambiata.ivory.scoobi.TestConfigurations

class DenseRowTextStorageSpec extends Specification with ScalaCheck { def is = s2"""
  Dense rows line up                   $rowsLineUp
  Dense rows stored correctly          $rowsStoredCorrectly
"""
  def rowsLineUp = {
    val features = List((0, FeatureId("ns1", "fid1"), FeatureMeta(StringEncoding, Some(CategoricalType), "")),
                        (1, FeatureId("ns1", "fid2"), FeatureMeta(IntEncoding, Some(ContinuousType), "")),
                        (2, FeatureId("ns1", "fid3"), FeatureMeta(BooleanEncoding, Some(CategoricalType), "")),
                        (3, FeatureId("ns1", "fid4"), FeatureMeta(DoubleEncoding, Some(NumericalType), "")))
    val facts = List(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 1, 1), Time(0), "abc"),
                     IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 1, 1), Time(0), 123),
                     BooleanFact("eid1", FeatureId("ns1", "fid3"), Date(2012, 1, 1), Time(0), true))

    DenseRowTextStorageV1.makeDense(facts, features, "☠") must_== List("abc", "123", "true", "☠")
  }

  def rowsStoredCorrectly = {
    implicit val sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration
    val directory = path(TempFiles.createTempDir("denserowtextstorer").getPath)

    val dict = Dictionary(Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, Some(CategoricalType), ""),
                                      FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, Some(ContinuousType), ""),
                                      FeatureId("ns1", "fid3") -> FeatureMeta(BooleanEncoding, Some(CategoricalType), ""),
                                      FeatureId("ns1", "fid4") -> FeatureMeta(DoubleEncoding, Some(NumericalType), ""),
                                      FeatureId("ns1", "fid5") -> FeatureMeta(StructEncoding(Map("a" -> StructEncodedValue(StringEncoding))), None, ""),
                                      FeatureId("ns1", "fid6") -> FeatureMeta(ListEncoding(StringEncoding), None, "")
    ))

    val facts = fromLazySeq(
                  Seq(BooleanFact("eid1", FeatureId("ns1", "fid3"), Date(2012, 1, 1), Time(0), true),
                      StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 1, 1), Time(0), "abc"),
                      IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 1, 1), Time(0), 123),
                      Fact.newFact("eid1", "ns1", "fid5", Date(2012, 3, 1), Time(0), StructValue(Map("a" -> StringValue("b")))),
                      Fact.newFact("eid1", "ns1", "fid6", Date(2012, 3, 1), Time(0), ListValue(List(StringValue("c")))),
                      DoubleFact("eid2", FeatureId("ns1", "fid4"), Date(2012, 2, 2), Time(123), 2.0),
                      IntFact("eid2", FeatureId("ns1", "fid2"), Date(2012, 3, 1), Time(0), 9)
                  ))

    val res = DenseRowTextStorageV1.DenseRowTextStorer(directory, dict).storeScoobi(facts).run.toList
    // Note that there is no sign of structs/lists in either the dictionary or dense row - this is intentional
    (res must_== List("eid1|abc|123|true|NA", "eid2|NA|9|NA|2.0")) and
    (DenseRowTextStorageV1.indexDictionary(dict).map(e => (e._1, e._2)) must_== List(
      0 -> FeatureId("ns1", "fid1"),
      1 -> FeatureId("ns1", "fid2"),
      2 -> FeatureId("ns1", "fid3"),
      3 -> FeatureId("ns1", "fid4")
    ))
  }
}
