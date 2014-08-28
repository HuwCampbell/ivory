package com.ambiata.ivory.operation.validation

import org.specs2._
import org.specs2.matcher.{ThrownExpectations, FileMatchers}
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.SimpleJobs
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import java.io.File
import java.net.URI
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.OldIdentifier
import com.ambiata.ivory.scoobi._, WireFormats._, FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import IvoryStorage._

import com.ambiata.ivory.core._, Arbitraries._


class ValidateSpec extends Specification with ThrownExpectations with FileMatchers with ScalaCheck { def is = s2"""

  Can validate with correct encoding                     $valid
  Can validate with incorrect encoding                   $invalid

  Validate feature store                                 $featureStore
  Validate fact set                                      $factSet

  """

  def valid = prop((e: EncodingAndValue) =>
    Validate.validateEncoding(e.value, e.enc).toEither must beRight
  )

  def invalid = prop((e: EncodingAndValue, e2: Encoding) => (e.enc != e2 && !isCompatible(e, e2)) ==> {
    Validate.validateEncoding(e.value, e2).toEither must beLeft
  })


  def featureStore = {
    implicit val sc: ScoobiConfiguration = ScoobiConfiguration()
    implicit val fs = sc.fileSystem

    val directory = path(TempFiles.createTempDir("validation").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)
    val outpath = directory + "/out"
    val factsetId1 = FactsetId.initial
    val factsetId2 = factsetId1.next.get

    val dict = Dictionary(List(Definition.concrete(FeatureId(Name("ns1"), "fid1"), DoubleEncoding, Some(NumericalType), "desc", Nil),
                               Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Some(NumericalType), "desc", Nil),
                               Definition.concrete(FeatureId(Name("ns2"), "fid3"), BooleanEncoding, Some(CategoricalType), "desc", Nil)))

    val facts1 = fromLazySeq(Seq(StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                                 IntFact("eid1",     FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
                                 BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true)))
    val facts2 = fromLazySeq(Seq(StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "def")))

    val factset1 = Factset(FactsetId(OldIdentifier("00000")), Partitions(List(Partition(Name("ns1"), Date(2012, 10, 1)), Partition(Name("ns2"), Date(2012, 3, 20)))))
    val factset2 = Factset(FactsetId(OldIdentifier("00001")), Partitions(List(Partition(Name("ns1"), Date(2012, 10, 1)))))

    persist(facts1.toIvoryFactset(repo, factsetId1, None), facts2.toIvoryFactset(repo, factsetId2, None))
    writeFactsetVersion(repo, List(factsetId1, factsetId2)) must beOk

    val store = FeatureStore.fromList(FeatureStoreId.initial, List(factset1, factset2)).get

    ValidateStoreHdfs(repo, store, dict, false).exec(new Path(outpath)).run(sc) must beOk

    val res = fromTextFile(outpath).run.toList
                               println(res)
    res must have size(1)
    res must contain("Not a valid double!")
    res must contain("eid1")
    res must contain("ns1")
    res must contain("fid1")
    res must contain("00000")
  }

  def factSet = {
    implicit val sc: ScoobiConfiguration = ScoobiConfiguration()
    implicit val fs = sc.fileSystem

    val directory = path(TempFiles.createTempDir("validation").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)
    val outpath = directory + "/out"
    val factsetId1 = FactsetId.initial
    val factsetId2 = factsetId1.next.get

    val dict = Dictionary(List(Definition.concrete(FeatureId(Name("ns1"), "fid1"), DoubleEncoding, Some(NumericalType), "desc", Nil),
                               Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Some(NumericalType), "desc", Nil),
                               Definition.concrete(FeatureId(Name("ns2"), "fid3"), BooleanEncoding, Some(CategoricalType), "desc", Nil)))

    val facts1 = fromLazySeq(Seq(StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                                 IntFact("eid1",     FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
                                 BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true)))

    facts1.toIvoryFactset(repo, factsetId1, None).persist
    writeFactsetVersion(repo, List(factsetId1)) must beOk

    ValidateFactSetHdfs(repo, factsetId1, dict).exec(new Path(outpath)).run(sc) must beOk

    val res = fromTextFile(outpath).run.toList
    res must have size(1)
    res must contain("Not a valid double!")
    res must contain("eid1")
    res must contain("ns1")
    res must contain("fid1")
  }

  // A small subset of  encoded values are valid for different optional/empty Structs/Lists
  private def isCompatible(e1: EncodingAndValue, e2: Encoding): Boolean =
    (e1, e2) match {
      case (EncodingAndValue(_, StructValue(m)), StructEncoding(v)) => m.isEmpty && v.forall(_._2.optional)
      case (EncodingAndValue(_, ListValue(l)), ListEncoding(e))     => l.forall(v => isCompatible(EncodingAndValue(e, v), e))
      case _ => false
    }
}
