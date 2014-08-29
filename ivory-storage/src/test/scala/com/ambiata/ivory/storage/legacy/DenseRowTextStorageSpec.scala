package com.ambiata.ivory.storage.legacy

import com.ambiata.mundane.control.ResultT
import com.ambiata.mundane.io.Temporary
import com.nicta.scoobi.Scoobi._
import org.specs2._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._, FactFormats._
import com.ambiata.ivory.scoobi.TestConfigurations
import com.ambiata.mundane.testing.ResultTIOMatcher._

import scalaz.effect.IO

class DenseRowTextStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Dense rows line up                   $rowsLineUp
  Dense rows stored correctly          $rowsStoredCorrectly
  The dictionary can be indexed        $index

"""
  def rowsLineUp = {
    val features = List((0, FeatureId(Name("ns1"), "fid1"), Definition.concrete(FeatureId(Name("ns1"), "fid1"), StringEncoding, Some(CategoricalType), "", List("☠"))),
                        (1, FeatureId(Name("ns1"), "fid2"), Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Some(ContinuousType), "", List("☠"))),
                        (2, FeatureId(Name("ns1"), "fid3"), Definition.concrete(FeatureId(Name("ns1"), "fid3"), BooleanEncoding, Some(CategoricalType), "", List("☠"))),
                        (3, FeatureId(Name("ns1"), "fid4"), Definition.concrete(FeatureId(Name("ns1"), "fid4"), DoubleEncoding, Some(NumericalType), "", List("☠"))))
    val facts = List(StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 1, 1), Time(0), "abc"),
                     IntFact("eid1",     FeatureId(Name("ns1"), "fid2"), Date(2012, 1, 1), Time(0), 123),
                     BooleanFact("eid1", FeatureId(Name("ns1"), "fid3"), Date(2012, 1, 1), Time(0), true))

    DenseRowTextStorageV1.makeDense(facts, features, "☠") must_== List("abc", "123", "true", "☠")
  }

  def rowsStoredCorrectly = {
    implicit val sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration
    Temporary.using { dir =>
      val directory = dir.path

      val facts = fromLazySeq(
        Seq(BooleanFact("eid1", FeatureId(Name("ns1"), "fid3"), Date(2012, 1, 1), Time(0), true),
          StringFact("eid1",    FeatureId(Name("ns1"), "fid1"), Date(2012, 1, 1), Time(0), "abc"),
          IntFact("eid1",       FeatureId(Name("ns1"), "fid2"), Date(2012, 1, 1), Time(0), 123),
          Fact.newFact("eid1", "ns1", "fid5", Date(2012, 3, 1), Time(0), StructValue(Map("a" -> StringValue("b")))),
          Fact.newFact("eid1", "ns1", "fid6", Date(2012, 3, 1), Time(0), ListValue(List(StringValue("c")))),
          DoubleFact("eid2", FeatureId(Name("ns1"), "fid4"), Date(2012, 2, 2), Time(123), 2.0),
          IntFact("eid2",    FeatureId(Name("ns1"), "fid2"), Date(2012, 3, 1), Time(0), 9)
        ))

      ResultT.ok[IO, List[String]](DenseRowTextStorageV1.DenseRowTextStorer(directory, dict).storeScoobi(facts).run.toList)

    } must
      // Note that there is no sign of structs/lists in either the dictionary or dense row - this is intentional
      beOkValue(List("eid1|abc|123|true|NA", "eid2|NA|9|NA|2.0"))
  }

  def index =
    DenseRowTextStorageV1.indexDictionary(dict).map(e => (e._1, e._2)) must_== List(
      0 -> FeatureId(Name("ns1"), "fid1"),
      1 -> FeatureId(Name("ns1"), "fid2"),
      2 -> FeatureId(Name("ns1"), "fid3"),
      3 -> FeatureId(Name("ns1"), "fid4"))

  val dict = Dictionary(List(Definition.concrete(FeatureId(Name("ns1"), "fid1"), StringEncoding, Some(CategoricalType), "", List("☠")),
              Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Some(ContinuousType), "", List("☠")),
              Definition.concrete(FeatureId(Name("ns1"), "fid3"), BooleanEncoding, Some(CategoricalType), "", List("☠")),
              Definition.concrete(FeatureId(Name("ns1"), "fid4"), DoubleEncoding, Some(NumericalType), "", List("☠")),
              Definition.concrete(FeatureId(Name("ns1"), "fid5"), StructEncoding(Map("a" -> StructEncodedValue(StringEncoding))), None, "", List("☠")),
              Definition.concrete(FeatureId(Name("ns1"), "fid6"), ListEncoding(StringEncoding), None, "", List("☠"))
              ))
}
