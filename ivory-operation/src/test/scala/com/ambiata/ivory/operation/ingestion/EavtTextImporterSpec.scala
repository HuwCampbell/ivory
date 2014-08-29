package com.ambiata.ivory.operation.ingestion

import java.io.File

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
import org.specs2.matcher.{FileMatchers, ThrownExpectations}
import org.specs2.specification.{FixtureExample, Fixture}
import com.ambiata.ivory.operation.ingestion.thrift._
import scalaz.effect.IO
import scalaz.{Name => _, DList => _, _}, Scalaz._

class EavtTextImporterSpec extends Specification with ThrownExpectations with FileMatchers with FixtureExample[Setup] { def is = s2"""

  MR job runs and creates expected text data $textData

  MR job runs and creates expected thrift data $thriftData

  When there are errors, they must be saved as a Thrift record containing the full record + the error message $errorMessage

  """

  def textData = { setup: Setup =>
    import setup._

    saveInputFile >>
    run(setup, TextFormat) must beOkValue(expected)
  }

  def thriftData = { setup: Setup =>
    val serializer = ThriftSerialiser()
    import setup._

    SequenceUtil.writeBytes(directory </> "input" </> "ns1" </> java.util.UUID.randomUUID().toString, None) {
      writer => ResultT.safe(expected.map(Conversion.fact2thrift).map(fact => serializer.toBytes(fact)).foreach(writer))
    }.run(setup.sc) >>
    run(setup, ThriftFormat) must beOkValue(expected)
  }

  def errorMessage = { setup: Setup =>
    import setup._
    // save an input file containing errors
    val errors = base </> "errors"

    // run the scoobi job to import facts on Hdfs
    (for {
      _         <- saveInputFileWithErrors
      errorPath <- Reference.hdfsPath(errors)
      _         <- EavtTextImporter.onStore(repository, dictionary, FactsetId.initial, List(ns1), None, input, errors, DateTimeZone.getDefault, List(ns1 -> 1.mb), 128.mb, TextFormat)
      ret = valueFromSequenceFile[ParseError](errorPath.toString).run
    } yield ret) must beOkLike(_ must not(beEmpty))
  }

  def run(setup: Setup, format: Format) = {
    import setup._

    val errors = base </> "errors"
    // run the scoobi job to import facts on Hdfs
    EavtTextImporter.onStore(repository, dictionary, FactsetId.initial, List(ns1), None, input, errors, DateTimeZone.getDefault, List(ns1 -> 1.mb), 128.mb, format) >>
      factsFromIvoryFactset(repository, FactsetId.initial).map(_.run.collect { case \/-(r) => r }.toSet).run(sc)
  }

  def fixture[R : AsResult](f: Setup => R): Result =
    Temporary.using { dir =>
       ResultT.ok[IO, Result](AsResult(f(new Setup(dir))))
    } must beOkLike(r => r.isSuccess aka r.message must beTrue)

  // This needs to be a function otherwise Scoobi will serialise with xstream :(
  def expected = Set(
    StringFact("pid1", FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1),  Time(10), "v1"),
    IntFact(   "pid1", FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 15), Time(20), 2),
    DoubleFact("pid1", FeatureId(Name("ns1"), "fid3"), Date(2012, 3, 20),  Time(30), 3.0))

  val ns1 = Name("ns1")
}

class Setup(val directory: FilePath) {
  implicit def sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration
  implicit lazy val fs = sc.fileSystem

  lazy val base = Reference(HdfsStore(sc, directory), FilePath.root)
  lazy val input = base </> "input"
  lazy val namespaced = (input </> "ns1").path
  lazy val repository = Repository.fromHdfsPath(directory </> "repo", sc)

  val dictionary =
    Dictionary(
      List(Definition.concrete(FeatureId(Name("ns1"), "fid1"), StringEncoding, Some(CategoricalType), "abc", Nil),
           Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding,    Some(NumericalType),   "def", Nil),
           Definition.concrete(FeatureId(Name("ns1"), "fid3"), DoubleEncoding, Some(NumericalType),   "ghi", Nil)))

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

  def save(path: FilePath, raw: List[String]) =
    (base </> path </> "part").run(store => fp => store.linesUtf8.write(fp, raw))
}
