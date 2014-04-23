package com.ambiata.ivory.chord

import org.specs2._
import org.specs2.matcher.FileMatchers
import scalaz.{DList => _, _}, Scalaz._
import org.joda.time.LocalDate
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.mutable._
import com.nicta.scoobi.testing.SimpleJobs
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.TempFiles
import java.io.File
import java.net.URI
import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.storage._
import IvoryStorage._

class ChordSpec extends HadoopSpecification with SimpleJobs with FileMatchers {
  override def isCluster = false

  "Can extract expected facts" >> { implicit sc: ScoobiConfiguration =>
    implicit val fs = sc.fileSystem

    val directory = path(TempFiles.createTempDir("chord").getPath)
    val repo = Repository.fromHdfsPath(new Path(directory + "/repo"))
    val outpath = directory + "/out"
    val errpath = directory + "/err"

    val entities = Seq("eid1|2012-09-15", "eid2|2012-12-01", "eid1|2012-11-01")

    lazy val entitiesFile = new File(directory + "/entities")
    TempFiles.writeLines(entitiesFile, entities, isRemote)

    val dict = Dictionary("dict1", Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, CategoricalType, "desc"),
                                       FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding, NumericalType, "desc"),
                                       FeatureId("ns2", "fid3") -> FeatureMeta(BooleanEncoding, CategoricalType, "desc")))

    dictionaryToIvory(repo, dict, dict.name).run(sc) must beOk

    val facts1 = DList(StringFact("eid1", FeatureId("ns1", "fid1"), new LocalDate(2012, 10, 1), 0, "abc"),
                       StringFact("eid1", FeatureId("ns1", "fid1"), new LocalDate(2012, 9, 1), 0, "def"),
                       IntFact("eid2", FeatureId("ns1", "fid2"), new LocalDate(2012, 10, 1), 0, 10),
                       IntFact("eid2", FeatureId("ns1", "fid2"), new LocalDate(2012, 11, 1), 0, 11),
                       BooleanFact("eid3", FeatureId("ns2", "fid3"), new LocalDate(2012, 3, 20), 0, true))
    val facts2 = DList(StringFact("eid1", FeatureId("ns1", "fid1"), new LocalDate(2012, 9, 1), 0, "ghi"))

    persist(facts1.toIvoryFactset(repo, "factset1"), facts2.toIvoryFactset(repo, "factset2"))
    writeFactsetVersion(repo, List("factset1", "factset2")).run(sc) must beOk
    
    storeToIvory(repo, FeatureStore(List(FactSet("factset1", 1), FactSet("factset2", 2))), "store1").run(sc) must beOk

    val storer = DelimitedFactTextStorage.DelimitedFactTextStorer(new Path(outpath))
    Chord.onHdfs(repo.path, "store1", "dict1", new Path(entitiesFile.toString), new Path(outpath), new Path(errpath), storer).run(sc) must beOk

    val res = fromTextFile(outpath).run.toList
    res must_== List("eid1:2012-09-15|ns1:fid1|def|2012-09-01 00:00:00", "eid1:2012-11-01|ns1:fid1|abc|2012-10-01 00:00:00", "eid2:2012-12-01|ns1:fid2|11|2012-11-01 00:00:00")
  }
}
