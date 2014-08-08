package com.ambiata.ivory.tools

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
import com.ambiata.ivory.scoobi._, WireFormats._, FactFormats._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import IvoryStorage._

class ValidateSpec extends Specification with ThrownExpectations with FileMatchers { def is = s2"""

  Validate feature store $e1
  Validate fact set $e2

  """

  def e1 = {
    implicit val sc: ScoobiConfiguration = ScoobiConfiguration()
    implicit val fs = sc.fileSystem

    val directory = path(TempFiles.createTempDir("validation").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)
    val outpath = directory + "/out"
    val factsetId1 = FactsetId.initial
    val factsetId2 = factsetId1.next.get

    val dict = Dictionary(Map(FeatureId("ns1", "fid1") -> FeatureMeta(DoubleEncoding, Some(NumericalType), "desc"),
                              FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, Some(NumericalType), "desc"),
                              FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, Some(CategoricalType), "desc")))

    val facts1 = fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                                 IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10),
                                 BooleanFact("eid1", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)))
    val facts2 = fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "def")))

    persist(facts1.toIvoryFactset(repo, factsetId1, None), facts2.toIvoryFactset(repo, factsetId2, None))
    writeFactsetVersion(repo, List(factsetId1, factsetId2)) must beOk

    val store = FeatureStore(List(PrioritizedFactset(factsetId1, Priority(1)), PrioritizedFactset(factsetId2, Priority(2))))

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

  def e2 = {
    implicit val sc: ScoobiConfiguration = ScoobiConfiguration()
    implicit val fs = sc.fileSystem

    val directory = path(TempFiles.createTempDir("validation").getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)
    val outpath = directory + "/out"
    val factsetId1 = FactsetId.initial
    val factsetId2 = factsetId1.next.get

    val dict = Dictionary(Map(FeatureId("ns1", "fid1") -> FeatureMeta(DoubleEncoding, Some(NumericalType), "desc"),
                              FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, Some(NumericalType), "desc"),
                              FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, Some(CategoricalType), "desc")))

    val facts1 = fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
                                 IntFact("eid1", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10),
                                 BooleanFact("eid1", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)))

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
}
