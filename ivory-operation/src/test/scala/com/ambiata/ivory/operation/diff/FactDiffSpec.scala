package com.ambiata.ivory.operation.diff

import org.specs2._
import org.specs2.matcher.{ThrownExpectations, FileMatchers}
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import com.ambiata.poacher.hdfs._
import FactFormats._

class FactDiffSpec extends Specification with ThrownExpectations with FileMatchers { def is = s2"""

  FactDiff finds difference with all facts $e1
  FactDiff finds no difference $e2
  FactDiff finds difference with structs   $structs
  FactDiff finds difference with list      $list

  """

  def e1 = {
    val facts1 = fromLazySeq(Seq(StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                                 IntFact("eid1",     FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
                                 BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true)))
    val facts2 = fromLazySeq(Seq(StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abcd"),
                                 IntFact("eid1",     FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 101),
                                 BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), false)))
    val (output, sc) = diff(facts1, facts2)
    fromTextFile(output).run(sc).toList must have size 6
  }

  def e2 = {
    val facts = fromLazySeq(Seq(
      StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
      IntFact("eid1",     FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
      BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true))
    )

    val (output, sc) = diff(facts, facts)
    Hdfs.readWith(new Path(output), is => Streams.read(is)).run(sc) must beOkValue("")
  }

  def structs = {
    def fact(v: String) =
      fromLazySeq(Seq(Fact.newFact("eid1", "ns1", "fid1", Date(2012, 10, 1), Time(0), StructValue(Map("a" -> StringValue(v))))))

    val (output, sc) = diff(fact("b"), fact("c"))
    fromTextFile(output).run(sc).toList must have size 2
  }

  def list = {
    def fact(v: String) =
      fromLazySeq(Seq(Fact.newFact("eid1", "ns1", "fid1", Date(2012, 10, 1), Time(0), ListValue(List(StringValue(v))))))

    val (output, sc) = diff(fact("b"), fact("c"))
    fromTextFile(output).run(sc).toList must have size 2
  }

  private def diff(facts1: DList[Fact], facts2: DList[Fact]): (String, ScoobiConfiguration) = {
    implicit val sc: ScoobiConfiguration = ScoobiConfiguration()

    val directory: String = path(TempFiles.createTempDir("factdiff").getPath).pp
    val input1 = directory + "/1"
    val input2 = directory + "/2"
    val output = directory + "/out"

    persist(PartitionFactThriftStorageV1.PartitionedFactThriftStorer(input1, None).storeScoobi(facts1),
            PartitionFactThriftStorageV1.PartitionedFactThriftStorer(input2, None).storeScoobi(facts2))

    FactDiff.partitionFacts(input1, input2, output).run(sc) must beOk
    (output, sc)
  }
}
