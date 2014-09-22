package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.core.thrift.ThriftSerialiser
import com.ambiata.ivory.scoobi.{FactFormats, SequenceUtil, TestConfigurations}
import com.ambiata.ivory.storage.legacy.IvoryStorage._
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
import scalaz.{Name => _, _}, scalaz.effect._

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

class Setup(val directory: DirPath) extends MustThrownMatchers {
  implicit def sc: ScoobiConfiguration = TestConfigurations.scoobiConfiguration
  implicit lazy val fs = sc.fileSystem

  lazy val base = Reference(HdfsStore(sc, directory), DirPath.Root)
  lazy val input = base </> "input"
  lazy val namespaced = input </> "ns1"
  lazy val repository = HdfsRepository(directory </> "repo", IvoryConfiguration.fromScoobiConfiguration(sc))
  lazy val errors = base </> "errors"
  lazy val ns1 = Name("ns1")

  val dictionary =
    Dictionary(
      List(Definition.concrete(FeatureId(ns1, "fid1"), StringEncoding, Some(CategoricalType), "abc", Nil),
           Definition.concrete(FeatureId(ns1, "fid2"), IntEncoding,    Some(NumericalType),   "def", Nil),
           Definition.concrete(FeatureId(ns1, "fid3"), DoubleEncoding, Some(NumericalType),   "ghi", Nil)))

  // This needs to be a function otherwise Scoobi will serialise with xstream :(
  def expected = List(
    StringFact("pid1", FeatureId(ns1, "fid1"), Date(2012, 10, 1),  Time(10), "v1"),
    IntFact(   "pid1", FeatureId(ns1, "fid2"), Date(2012, 10, 15), Time(20), 2),
    DoubleFact("pid1", FeatureId(ns1, "fid3"), Date(2012, 3, 20),  Time(30), 3.0))

  def saveTextInputFile = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
                   "pid1|fid2|2|2012-10-15 00:00:20",
                   "pid1|fid3|3.0|2012-03-20 00:00:30")
    save(namespaced, raw) must beOk
  }

  def saveThriftInputFile = {
    import com.ambiata.ivory.operation.ingestion.thrift._
    val serializer = ThriftSerialiser()

    SequenceUtil.writeBytes(directory </> "input" </> "ns1" <|> FileName.unsafe(java.util.UUID.randomUUID().toString), None) {
      writer => ResultT.safe(expected.map(Conversion.fact2thrift).map(fact => serializer.toBytes(fact)).foreach(writer))
    }.run(sc) must beOk
  }
  
  def saveTextInputFileWithErrors = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
                   "pid1|fid2|x|2012-10-15 00:00:20",
                   "pid1|fid3|3.0|2012-03-20 00:00:30")
    save(namespaced, raw) must beOk
  }

  def save(path: ReferenceIO, raw: List[String]) =
    ReferenceStore.writeLines(path </> "part", raw)

  def importAs(format: Format) = {
    val action =
      for {
        inputPath  <- Reference.hdfsPath(input)
        errorsPath <- Reference.hdfsPath(errors)
        _          <- EavtTextImporter(repository, namespace = None, optimal = 128.mb, format).
                        runJob(repository, dictionary, FactsetId.initial, inputPath, errorsPath, List(ns1 -> 1.mb), DateTimeZone.getDefault)
        _ <- writeFactsetVersion(repository, List(FactsetId.initial))
      } yield ()

    action must beOk
  }

  def theImportMustBeOk = 
    factsFromIvoryFactset(repository, FactsetId.initial).map(_.run.collect { case \/-(r) => r }).run(sc) must beOkLike(_.toSet must_== expected.toSet)

  def thereMustBeErrors =
    valueFromSequenceFile[ParseError]((directory </> "errors").path).run(sc) must not(beEmpty)
}

class EavtTextImporterPureSpec extends Specification with ScalaCheck { def is = s2"""

  Validate namespaces success                        $validateNamespacesSuccess
  Validate namespaces fail                           $validateNamespacesFail
"""

  def validateNamespacesSuccess = prop((dict: Dictionary) => {
    EavtTextImporter.validateNamespaces(dict, dict.byFeatureId.keys.toList.map(_.namespace)).toEither must beRight
  })

  def validateNamespacesFail = prop((dict: Dictionary, name: Name, names: List[Name]) => {
    val allNames = (name :: names).filter(dict.forNamespace(_).definitions.isEmpty)
    EavtTextImporter.validateNamespaces(dict, allNames).toEither must beLeft
  })
}
