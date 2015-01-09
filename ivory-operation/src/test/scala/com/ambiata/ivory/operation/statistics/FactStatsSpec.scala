package com.ambiata.ivory.operation.statistics

import com.ambiata.mundane.control.ResultT
import com.nicta.scoobi.testing.TestFiles
import org.specs2._
import org.specs2.execute.{Result, AsResult}
import org.specs2.matcher.{ThrownExpectations, FileMatchers}
import com.ambiata.ivory.core._

import scalaz.effect.IO
import argonaut._, Argonaut._

class FactStatsSpec extends Specification with ThrownExpectations with FileMatchers { def is = sequential ^ s2"""

  FactStats can get the histogram and numerical statistics for of a set of facts      $e1
  FactStats can be written to and from JSON                                           $e2

  """

  // Note: On merge with snapshot meta branch, these should be deleted.
  implicit def DateJsonCodec: CodecJson[Date] = CodecJson.derived(
    EncodeJson(_.int.asJson),
    DecodeJson.optionDecoder(_.as[Int].toOption.flatMap(Date.fromInt), "Date"))

  type KeyInfo        = (String, Date)
  type Histogram      = Map[String,Int]
  type NumericalStats = (Long, Double, Double)
  type FactStatEncode = (KeyInfo, Either[NumericalStats, Histogram])

  val facts1 = Seq(StringFact("eid1",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "horus"),
                   StringFact("eid2",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "horus"),
                   StringFact("eid3",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "anubis"),
                   StringFact("eid4",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "anubis"),
                   StringFact("eid4",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "anubis"),
                   StringFact("eid5",  FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "isis")
                  )
  val facts2 = Seq(IntFact("eid1",     FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10, 2), Time(0), 1),
                   IntFact("eid3",     FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10, 2), Time(0), 9)
               )

  def e1 = {
      FactStats.genStats((FeatureId(Namespace("ns1"), "fid1"), Date(2012, 10, 1)), facts1.toIterable) ==== List(((FeatureId(Namespace("ns1"), "fid1").toString, Date(2012, 10, 1)), Right(Map("horus" -> 2, "anubis" -> 3, "isis" -> 1))))
      FactStats.genStats((FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10, 2)), facts2.toIterable) ==== List(((FeatureId(Namespace("ns1"), "fid2").toString, Date(2012, 10, 2)), Right(Map("1" -> 1, "9" -> 1))),
                                                                                                                          ((FeatureId(Namespace("ns1"), "fid2").toString, Date(2012, 10, 2)), Left( (2, 5.0, 4.0))))
  }

  def e2 = {
    val f = FactStats.genStats((FeatureId(Namespace("ns1"), "fid2"), Date(2012, 10, 2)), facts2.toIterable)
    Parse.decodeOption[List[FactStatEncode]](f.asJson.nospaces) must beSome(f)
  }
}
