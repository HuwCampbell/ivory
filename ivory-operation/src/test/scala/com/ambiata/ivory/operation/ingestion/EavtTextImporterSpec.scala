package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftSerialiser
import com.ambiata.ivory.scoobi.{FactFormats, SequenceUtil, TestConfigurations}
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import FactFormats._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.HdfsStore
import com.nicta.scoobi.Scoobi._
import org.joda.time.DateTimeZone
import org.specs2._
import org.specs2.execute.{AsResult, Result}
import org.specs2.matcher.{MustThrownMatchers, FileMatchers, ThrownExpectations}
import org.specs2.specification._
import scalaz.\/-
import scalaz.effect.IO

class EavtTextImporterSpec extends Specification with ThrownExpectations with FileMatchers with FixtureExample[Setup] { def is = s2"""
 
 The Eavt text import can import text or Thrift facts

  MR job runs and creates expected text data   $text
  MR job runs and creates expected thrift data $thrift
  When there are errors, they must be saved as a Thrift record containing the full record + the error message $withErrors

  """

  def text = { setup: Setup =>
    setup.saveTextInputFile
    setup.importAs(TextFormat)
    setup.theImportMustBeOk
  }

  def thrift = { setup: Setup =>
    setup.saveThriftInputFile
    setup.importAs(ThriftFormat)
    setup.theImportMustBeOk
  }

  def withErrors = { setup: Setup =>
    // save an input file containing errors
    setup.saveTextInputFileWithErrors
    setup.importAs(TextFormat)
    setup.thereMustBeErrors
  }

  def fixture[R : AsResult](f: Setup => R): Result =
    Temporary.using { dir =>
      ResultT.ok[IO, Result](AsResult(f(new Setup(dir))))
    } must beOkLike(r => r.isSuccess aka r.message must beTrue)
}

class Setup(val directory: FilePath) extends MustThrownMatchers {
  implicit def sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration
  implicit lazy val fs = sc.fileSystem

  val base = Reference(HdfsStore(sc, directory), FilePath.root)
  val input = base </> "input"
  val namespaced = (directory </> (input </> "/ns1").path).path
  val repository = Repository.fromHdfsPath(directory </> "repo", sc)
  val errors = base </> "errors"
  val ns1 = Name("ns1")

  val dictionary =
    Dictionary(
      List(Definition.concrete(FeatureId(Name("ns1"), "fid1"), StringEncoding, Some(CategoricalType), "abc", Nil),
           Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding,    Some(NumericalType),   "def", Nil),
           Definition.concrete(FeatureId(Name("ns1"), "fid3"), DoubleEncoding, Some(NumericalType),   "ghi", Nil)))

  // This needs to be a function otherwise Scoobi will serialise with xstream :(
  def expected = List(
    StringFact("pid1", FeatureId("ns1", "fid1"), Date(2012, 10, 1),  Time(10), "v1"),
    IntFact(   "pid1", FeatureId("ns1", "fid2"), Date(2012, 10, 15), Time(20), 2),
    DoubleFact("pid1", FeatureId("ns1", "fid3"), Date(2012, 3, 20),  Time(30), 3.0))

  def saveTextInputFile = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
      "pid1|fid2|2|2012-10-15 00:00:20",
      "pid1|fid3|3.0|2012-03-20 00:00:30")
    save(namespaced, raw)
  }

  def saveThriftInputFile = {
    import com.ambiata.ivory.operation.ingestion.thrift._
    val serializer = ThriftSerialiser()

    SequenceUtil.writeBytes(namespaced.toFilePath </> java.util.UUID.randomUUID().toString, None) {
      writer => ResultT.safe(expected.map(Conversion.fact2thrift).map(fact => serializer.toBytes(fact)).foreach(writer))
    }.run(sc) must beOk
  }
  
  def saveTextInputFileWithErrors = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
                   "pid1|fid2|x|2012-10-15 00:00:20",
                   "pid1|fid3|3.0|2012-03-20 00:00:30")
    save(namespaced, raw)
  }
  def save(path: FilePath, raw: List[String]) =
    (base </> path </> "part").run(store => fp => store.linesUtf8.write(fp, raw))

  def importAs(format: Format) = {
    // run the scoobi job to import facts on Hdfs
    EavtTextImporter.onStore(repository, dictionary, FactsetId.initial, None, input, errors, DateTimeZone.getDefault, List(ns1 -> 1.mb), 128.mb, format) must beOk
  }

  def theImportMustBeOk = 
    factsFromIvoryFactset(repository, FactsetId.initial).map(_.run.collect { case \/-(r) => r }).run(sc) must beOkLike(_.toSet must_== expected.toSet)

  def thereMustBeErrors =
    valueFromSequenceFile[ParseError]((directory </> "errors").path).run(sc) must not(beEmpty)
}
