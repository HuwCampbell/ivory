package com.ambiata.ivory.storage.legacy

import java.io.File

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.TempFiles
import com.nicta.scoobi.testing.TestFiles._
import org.specs2.matcher.MustThrownMatchers

import scalaz.{DList => _}

trait SampleFacts extends MustThrownMatchers {

  def createEntitiesFiles(directory: String)(implicit sc: ScoobiConfiguration) = {
    implicit val fs = sc.fileSystem
    val entities = Seq("eid1|2012-09-15", "eid2|2012-12-01", "eid1|2012-11-01")

    lazy val entitiesFile = new File(directory + "/entities")
    TempFiles.writeLines(entitiesFile, entities, isRemote)
  }

  def createDictionary(repo: HdfsRepository) = {
    val dict = Dictionary(Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, Some(CategoricalType), "desc"),
      FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, Some(NumericalType), "desc"),
      FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, Some(CategoricalType), "desc")))

    dictionaryToIvory(repo, dict) must beOk
    dict
  }

  def createFacts(repo: HdfsRepository)(implicit sc: ScoobiConfiguration) = {
    val facts1 =
      fromLazySeq(Seq(StringFact ("eid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1), Time(0), "abc"),
        stringFact1,
        intFact1,
        intFact2,
        booleanFact1))

    val facts2 =
      fromLazySeq(Seq(StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 9, 1), Time(0), "ghi")))

    persist(facts1.toIvoryFactset(repo, Factset("factset1"), None), facts2.toIvoryFactset(repo, Factset("factset2"), None))
    writeFactsetVersion(repo, List(Factset("factset1"), Factset("factset2"))) must beOk

    storeToIvory(repo, FeatureStore(List(PrioritizedFactset(Factset("factset1"), Priority(1)), PrioritizedFactset(Factset("factset2"), Priority(2)))), "store1") must beOk

  }

  def stringFact1  = StringFact("eid1", FeatureId("ns1", "fid1"), Date(2012, 9, 1),  Time(0), "def")
  def intFact1     = IntFact("eid2", FeatureId("ns1", "fid2"), Date(2012, 10, 1), Time(0), 10)
  def intFact2     = IntFact("eid2", FeatureId("ns1", "fid2"), Date(2012, 11, 1), Time(0), 11)
  def booleanFact1 = BooleanFact("eid3", FeatureId("ns2", "fid3"), Date(2012, 3, 20), Time(0), true)

  def createAll(dirName: String)(implicit sc: ScoobiConfiguration) = {
    val directory = path(TempFiles.createTempDir(dirName).getPath)
    val repo = Repository.fromHdfsPath(directory </> "repo", sc)

    createEntitiesFiles(directory)
    createDictionary(repo)
    createFacts(repo)
    directory
  }
}
