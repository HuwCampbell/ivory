package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.TemporaryLocations._
import com.ambiata.ivory.core.TemporaryRepositories._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.operation.ingestion.thrift.Conversion
import com.ambiata.ivory.mr.SequenceUtil
import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.legacy.SampleFacts
import com.ambiata.ivory.storage.metadata.DictionaryThriftStorage
import com.ambiata.ivory.storage.repository.Repositories
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.ResultT
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.notion.core.TemporaryType.Hdfs
import com.ambiata.poacher.mr.ThriftSerialiser
import com.nicta.scoobi.Scoobi._
import org.joda.time.DateTimeZone
import org.specs2.{ScalaCheck, Specification}
import scalaz.{Name=>_,Value=>_,_}, Scalaz._
import MemoryConversions._

class IngestSpec extends Specification with SampleFacts with ScalaCheck { def is = sequential ^ s2"""

 Facts can be ingested from either
   a directory named namespace/year/month/day containing fact files                       $partitionIngest ${tag("mr")}
   a directory containing fact files (and the namespace is specified on the command line) $namespaceIngest ${tag("mr")}

 Facts can be ingested from thrift format                                                 $thrift          ${tag("mr")}

"""

  def partitionIngest = {
    withRepository(Hdfs) { repository: Repository =>
      withIvoryLocationDir(Hdfs) { location =>
        Repositories.create(repository) >>
          DictionaryThriftStorage(repository).store(dictionary) >>
          IvoryLocation.writeUtf8Lines(location </> "ns1" </> "2012" </> "10" </> "1" </> "part-r-00000", sampleFacts.flatten.map(toEavt)) >>
          IvoryLocation.writeUtf8Lines(location </> "ns1" </> "2012" </> "10" </> "1" </> "part-r-00001", sampleFacts.flatten.map(toEavt)) >>
          Ingest.ingestFacts(repository, location, None, DateTimeZone.forID("Australia/Sydney"), 100.mb, TextFormat).run.run(IvoryRead.create)
      }
    } must beOk
  }

  def namespaceIngest = {
    withRepository(Hdfs) { repository: Repository =>
      withIvoryLocationDir(Hdfs) { location =>
        Repositories.create(repository) >>
          DictionaryThriftStorage(repository).store(dictionary) >>
          IvoryLocation.writeUtf8Lines(location </> "part-r-00000", sampleFacts.flatten.map(toEavt)) >>
          IvoryLocation.writeUtf8Lines(location </> "part-r-00001", sampleFacts.flatten.map(toEavt)) >>
          Ingest.ingestFacts(repository, location, Some(Name("ns1")), DateTimeZone.forID("Australia/Sydney"), 100.mb, TextFormat).run.run(IvoryRead.create)
      }
    } must beOk
  }

  def thrift = prop {(facts: FactsWithDictionary, fact: Fact) =>
    val serialiser = ThriftSerialiser()
    val ns = facts.cg.fid.namespace
    //  Lazy, but guaranteed to be bad so we always have at least one error
    val badFacts = List(fact.withFeatureId(facts.cg.fid).withValue(StructValue(Map("" -> StringValue("")))))
    withHdfsRepository { repository: HdfsRepository => for {
      _   <- Repositories.create(repository)
      loc <- Repository.tmpDir(repository)
      _   <- DictionaryThriftStorage(repository).store(facts.dictionary)
      _   <- SequenceUtil.writeBytes(repository.toIvoryLocation(loc) </> "part-r-00000", None) {
        write => ResultT.safe((facts.facts ++ badFacts).foreach(fact => write(serialiser.toBytes(Conversion.fact2thrift(fact)))))
      }.run(repository.scoobiConfiguration)
      fid <- Ingest.ingestFacts(repository, repository.toIvoryLocation(loc), Some(ns), DateTimeZone.forID("Australia/Sydney"), 100.mb, ThriftFormat).run.run(IvoryRead.create)
      } yield (
        valueFromSequenceFile[ThriftFact](repository.toIvoryLocation(Repository.namespace(fid, ns)).toHdfs + "/*/*/*/*").run(repository.scoobiConfiguration).toSet,
        valueFromSequenceFile[ThriftFact](repository.toIvoryLocation(Repository.errors).toHdfs + "/*/*").run(repository.scoobiConfiguration).size
      )
    } must beOkValue(facts.facts.map(_.toThrift).toSet -> badFacts.size)
  }.set(minTestsOk = 1, maxDiscardRatio = 10)

  def toEavt(fact: Fact) =
   List(fact.entity, fact.featureId.name, Value.toString(fact.value, None).get, toString(fact.datetime)).mkString("|")

  def toString(datetime: DateTime) =
    "2012-09-01T00:00:00"

  val dictionary =
    sampleDictionary append {
      Dictionary(List(Definition.concrete(FeatureId(Name("ns1"), "fid3"), BooleanEncoding, Mode.State, Some(CategoricalType), "desc", Nil)))
    }

}
