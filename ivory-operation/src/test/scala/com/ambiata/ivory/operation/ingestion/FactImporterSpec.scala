package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.mr.FactFormats
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import FactFormats._
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.poacher.mr.ThriftSerialiser
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.joda.time.DateTimeZone
import org.specs2._
import org.specs2.execute.{AsResult, Result}
import org.specs2.matcher.{MustThrownMatchers, FileMatchers, ThrownExpectations}
import org.specs2.specification._
import scalaz.{Name => _, _}, scalaz.effect._
import syntax.bind._


class FactImporterSpec extends Specification with FileMatchers with FixtureExample[Setup] { def is = s2"""

 The Eavt text import can import text or Thrift facts

  MR job runs and creates expected text data   $text
  MR job runs and creates expected thrift data $thrift
  When there are errors parsing text, they must be saved as a Thrift record containing the full record + the error message $withErrors
  When there are errors parsing thrift, they must be saved as a Thrift record containing the full record + the error message $thriftWithErrors

"""

  def text = { setup: Setup =>
    ((for {
      _ <- setup.saveTextInputFile
      _ <- setup.importAs(FileFormat.Text(Delimiter.Psv, TextEscaping.Delimited))
    } yield ()) must beOk) and setup.theImportMustBeOk
  }

  def thrift = { setup: Setup =>
    ((for {
      _ <- setup.saveThriftInputFile
      _ <- setup.importAs(FileFormat.Thrift)
    } yield ()) must beOk) and setup.theImportMustBeOk
  }

  def withErrors = { setup: Setup =>
    ((for {
      // save an input file containing errors
      _ <- setup.saveTextInputFileWithErrors
      _ <- setup.importAs(FileFormat.Text(Delimiter.Psv, TextEscaping.Delimited))
    } yield ()) must beOk) and setup.thereMustBeErrors
  }

  def thriftWithErrors = { setup: Setup =>
    ((for {
      _ <- setup.saveThriftInputFileWithErrors
      _ <- setup.importAs(FileFormat.Thrift)
    } yield ()) must beOk) and setup.thereMustBeErrors
  }

  def fixture[R : AsResult](f: Setup => R): Result =
    RepositoryBuilder.using { repo =>
      ResultT.ok[IO, Result]({
        AsResult(f(new Setup(repo)))
      })
    } must beOkLike(r => r.isSuccess aka r.message must beTrue)
}

class Setup(val repository: HdfsRepository) extends MustThrownMatchers {

  implicit val sc = repository.scoobiConfiguration
  lazy val input = repository.root </> "input"
  lazy val namespaced = input </> "ns1"
  lazy val errors = repository.root </> "errors"
  lazy val ns1 = Name("ns1")

  val dictionary =
    Dictionary(
      List(Definition.concrete(FeatureId(ns1, "fid1"), StringEncoding, Mode.State, Some(CategoricalType), "abc", Nil),
           Definition.concrete(FeatureId(ns1, "fid2"), IntEncoding,    Mode.State, Some(NumericalType),   "def", Nil),
           Definition.concrete(FeatureId(ns1, "fid3"), DoubleEncoding, Mode.State, Some(NumericalType),   "ghi", Nil)))

  // This needs to be a function otherwise Scoobi will serialise with xstream :(
  def expected = List(
    StringFact("pid1", FeatureId(ns1, "fid1"), Date(2012, 10, 1),  Time(10), "v1"),
    IntFact(   "pid1", FeatureId(ns1, "fid2"), Date(2012, 10, 15), Time(20), 2),
    DoubleFact("pid1", FeatureId(ns1, "fid3"), Date(2012, 3, 20),  Time(30), 3.0))

  def saveTextInputFile: RIO[Unit] = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
                   "pid1|fid2|2|2012-10-15 00:00:20",
                   "pid1|fid3|3.0|2012-03-20 00:00:30")
    save(namespaced, raw)
  }

  def saveThriftInputFile: RIO[Unit] =
    saveFactsAsThrift(expected)

  def saveFactsAsThrift(facts: List[Fact]): RIO[Unit] = {
    import com.ambiata.ivory.operation.ingestion.thrift._
    val serializer = ThriftSerialiser()
    TemporaryIvoryConfiguration.withConf(conf =>
      SequenceUtil.writeHdfsBytes((namespaced </> "input" </> "ns1" </> FileName(java.util.UUID.randomUUID)).location, conf.configuration, None) {
        writer => ResultT.safe(facts.map(Conversion.fact2thrift).map(fact => serializer.toBytes(fact)).foreach(writer))
      }.run(conf.configuration)
    )
  }

  def saveTextInputFileWithErrors: RIO[Unit] = {
    val raw = List("pid1|fid1|v1|2012-10-01 00:00:10",
                   "pid1|fid2|x|2012-10-15 00:00:20",
                   "pid1|fid3|3.0|2012-03-20 00:00:30",
                   "pid".padTo(256, '1') + "|fid1|v1|2012-10-01 00:00:10")
    save(namespaced, raw)
  }

  def saveThriftInputFileWithErrors: RIO[Unit] = {
    val bad = List(
      StringFact("pid".padTo(256, '1'), FeatureId(ns1, "fid1"), Date(2012, 10, 1),  Time(10), "v1"),
      StringFact("pid1", FeatureId(ns1, "fid2"), Date(2012, 10, 1),  Time(10), "v1"))
    saveFactsAsThrift(expected ++ bad)
  }

  def save(path: IvoryLocation, raw: List[String]): RIO[Unit] =
    IvoryLocation.writeUtf8Lines(path </> "part", raw)

  def importAs(format: FileFormat): RIO[Unit] =
    FactImporter
      .runJob(repository, 128.mb, dictionary, FactsetId.initial, List((format, None, input.toHdfsPath)), errors.toHdfsPath,
        None, RepositoryConfig.testing.copy(timezone = DateTimeZone.getDefault)) >>
    writeFactsetVersion(repository, List(FactsetId.initial))

  def theImportMustBeOk =
    factsFromIvoryFactset(repository, FactsetId.initial).map(_.run.collect { case \/-(r) => r }).run(sc) must beOkLike(_.toSet must_== expected.toSet)

  def thereMustBeErrors =
    valueFromSequenceFile[ParseError](errors.toHdfs).run(sc) must not(beEmpty)
}

class FactImporterPureSpec extends Specification with ScalaCheck { def is = s2"""

  Validate namespaces success                        $validateNamespacesSuccess
  Validate namespaces fail                           $validateNamespacesFail
"""

  def validateNamespacesSuccess = prop((dict: Dictionary) => {
    FactImporter.validateNamespaces(dict, dict.byFeatureId.keys.toList.map(_.namespace)).toEither must beRight
  })

  def validateNamespacesFail = prop((dict: Dictionary, names: List[Name]) => {
    // Lazy way of create at least one name that isn't in the dictionary
    val name = Name.unsafe(dict.definitions.map(_.featureId.namespace.name).mkString)
    val allNames = (name :: names).filter(dict.forNamespace(_).definitions.isEmpty)
    FactImporter.validateNamespaces(dict, allNames).toEither must beLeft
  })
}
