package com.ambiata.ivory.ingest

import org.specs2._
import org.specs2.matcher.FileMatchers
import scalaz.{DList => _, _}, Scalaz._, \&/._
import org.joda.time.DateTimeZone
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.mutable._
import com.nicta.scoobi.testing.TestFiles._
import com.nicta.scoobi.testing.{TempFiles, SimpleJobs}
import java.io.File
import java.net.URI
import org.apache.hadoop.fs.Path

import com.ambiata.mundane.parse.ListParser
import com.ambiata.mundane.testing.ResultTIOMatcher._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.TestConfigurations
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.mundane.io._
import org.specs2.specification.{Fixture, FixtureExample}
import org.specs2.execute.{Result, AsResult}
import MemoryConversions._

class EavtTextImporterSpec extends Specification with FileMatchers { def is = s2"""

  MR job runs and creates expected data $e1

  When there are errors, they must be saved as a Thrift record containing the full record + the error message $e2

  """
  def e1 = setup { setup: Setup =>
    import setup._

    saveInputFile

    val errors = base </> "errors"
    // run the scoobi job to import facts on Hdfs
    EavtTextImporter.onStore(repository, dictionary, FactsetId("00000"), List("ns1"), None, input, errors, DateTimeZone.getDefault, List("ns1" -> 1.mb), 128.mb, None) must beOk

    val expected = List(
      StringFact("pid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1),  Time(10), "v1"),
      IntFact(   "pid1", FeatureId("ns1", "fid2"), Date(2012, 10, 15), Time(20), 2),
      DoubleFact("pid1", FeatureId("ns1", "fid3"), Date(2012, 3, 20),  Time(30), 3.0))


    factsFromIvoryFactset(repository, FactsetId("00000")).map(_.run.collect { case \/-(r) => r}).run(sc) must beOkLike(_ must containTheSameElementsAs(expected))
  }

  def e2 = setup { setup: Setup =>
    import setup._
    // save an input file containing errors
    saveInputFileWithErrors
    val errors = base </> "errors"

    // run the scoobi job to import facts on Hdfs
    (for {
      errorPath <- Reference.hdfsPath(errors)
      _         <- EavtTextImporter.onStore(repository, dictionary, FactsetId("00000"), List("ns1"), None, input, errors, DateTimeZone.getDefault, List("ns1" -> 1.mb), 128.mb, None)
      ret = valueFromSequenceFile[ParseError](errorPath.toString).run
    } yield ret) must beOkLike(_ must not(beEmpty))
  }

  def setup: Fixture[Setup] = new Fixture[Setup] {
    def apply[R : AsResult](f: Setup => R): Result =
      AsResult(f(new Setup))
  }
}

class Setup() {
  implicit def sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration
  implicit lazy val fs = sc.fileSystem

  val directory = FilePath(TempFiles.createTempDir("eavtimporter").getPath)
  val base = Reference(HdfsStore(sc, directory), FilePath.root)
  val input = base </> "input"
  val namespaced = (directory </> (input </> "/ns1").path).path
  val repository = Repository.fromHdfsPath((directory </> "repo"), sc)

  val dictionary =
    Dictionary(
      Map(FeatureId("ns1", "fid1") -> FeatureMeta(StringEncoding, Some(CategoricalType), "abc"),
          FeatureId("ns1", "fid2") -> FeatureMeta(IntEncoding,    Some(NumericalType), "def"),
          FeatureId("ns1", "fid3") -> FeatureMeta(DoubleEncoding, Some(NumericalType), "ghi")))

  def saveInputFile = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
      "pid1|fid2|2|2012-10-15 00:00:20",
      "pid1|fid3|3.0|2012-03-20 00:00:30")
    save(namespaced, raw)
  }

  def saveInputFileWithErrors = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
                   "pid1|fid2|x|2012-10-15 00:00:20",
                   "pid1|fid3|3.0|2012-03-20 00:00:30")
    save(namespaced, raw)
  }

  def save(path: String, raw: List[String]) = {
    val f = new File(path)
    f.mkdirs()
    TempFiles.writeLines(new File(f, "part"), raw, isRemote)
  }

}
