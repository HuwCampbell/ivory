package com.ambiata.ivory.operation.diff

import com.ambiata.mundane.control.ResultT
import org.specs2._
import org.specs2.execute.{Result, AsResult}
import org.specs2.matcher.{ThrownExpectations, FileMatchers}
import org.apache.hadoop.fs.Path
import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.ivory.core.TemporaryLocations.withHdfsRepository

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import com.ambiata.poacher.hdfs._
import FactFormats._

import scalaz.effect.IO

class FactDiffSpec extends Specification with ThrownExpectations with FileMatchers { def is = sequential ^ s2"""

  FactDiff finds difference with all facts $e1
  FactDiff finds no difference             $e2
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
    diff(facts1, facts2) { (output, sc) =>
      fromTextFile(output).run(sc).toList must have size 6
    }
  }

  def e2 = {
    val facts = fromLazySeq(Seq(
      StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
      IntFact("eid1",     FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
      BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true))
    )

    diff(facts, facts) { (output, sc) =>
      Hdfs.readWith(new Path(output), is => Streams.read(is)).run(sc) must beOkValue("")
    }
  }

  def structs = {
    def fact(v: String) =
      fromLazySeq(Seq(Fact.newFact("eid1", "ns1", "fid1", Date(2012, 10, 1), Time(0), StructValue(Map("a" -> StringValue(v))))))

    diff(fact("b"), fact("c")) { (output, sc) =>
      fromTextFile(output).run(sc).toList must have size 2
    }
  }

  def list = {
    def fact(v: String) =
      fromLazySeq(Seq(Fact.newFact("eid1", "ns1", "fid1", Date(2012, 10, 1), Time(0), ListValue(List(StringValue(v))))))

    diff(fact("b"), fact("c")) { (output, sc) =>
      fromTextFile(output).run(sc).toList must have size 2
    }
  }

  private def diff[R : AsResult](facts1: DList[Fact], facts2: DList[Fact])(f: (String, ScoobiConfiguration) => R): Result = {
    implicit val sc: ScoobiConfiguration = ScoobiConfiguration()
    withHdfsRepository { repository =>
      val key1   = "in" / "1"
      val key2   = "in" / "2"
      val output = repository.toIvoryLocation(Key("out"))

      persist(PartitionFactThriftStorageV1.PartitionedFactThriftStorer(repository, key1, None).storeScoobi(facts1),
        PartitionFactThriftStorageV1.PartitionedFactThriftStorer(repository, key2, None).storeScoobi(facts2))

      FactDiff.partitionFacts(repository.toIvoryLocation(key1), repository.toIvoryLocation(key2), output).run(sc) must beOk
      ResultT.ok[IO, Result](AsResult(f(output.toHdfs, sc)))
    } must beOkLike(identity)
  }
}
