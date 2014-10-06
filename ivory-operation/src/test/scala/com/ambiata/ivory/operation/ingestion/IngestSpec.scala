package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.TemporaryLocations._
import com.ambiata.ivory.storage.legacy.SampleFacts
import com.ambiata.ivory.storage.metadata.DictionaryThriftStorage
import com.ambiata.ivory.storage.repository.Repositories
import com.ambiata.mundane.io._
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.specs2.Specification
import scalaz.{Name=>_,Value=>_,_}, Scalaz._
import MemoryConversions._
import com.ambiata.mundane.testing.ResultTIOMatcher._

class IngestSpec extends Specification with SampleFacts { def is = sequential ^ s2"""

 Facts can be ingested from either
   a directory named namespace/year/month/day containing fact files                       $partitionIngest
   a directory containing fact files (and the namespace is specified on the command line) $namespaceIngest

"""

  def partitionIngest = {
    withRepository(Hdfs) { repository: Repository =>
      withIvoryLocationDir(Hdfs) { location =>
        Repositories.create(repository) >>
          DictionaryThriftStorage(repository).store(dictionary) >>
          IvoryLocation.writeUtf8Lines(location </> "ns1" </> "2012" </> "10" </> "1" </> "part-r-00000", sampleFacts.flatten.map(toEavt)) >>
          IvoryLocation.writeUtf8Lines(location </> "ns1" </> "2012" </> "10" </> "1" </> "part-r-00001", sampleFacts.flatten.map(toEavt)) >>
          Ingest.ingestFacts(repository, location, None, DateTimeZone.forID("Australia/Sydney"), 100.mb, TextFormat)
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
          Ingest.ingestFacts(repository, location, Some(Name("ns1")), DateTimeZone.forID("Australia/Sydney"), 100.mb, TextFormat)
      }
    } must beOk
  }



  def toEavt(fact: Fact) =
   List(fact.entity, fact.featureId.name, Value.toString(fact.value, None).get, toString(fact.datetime)).mkString("|")

  def toString(datetime: DateTime) =
    "2012-09-01T00:00:00"

  val dictionary =
    sampleDictionary append {
      Dictionary(List(Definition.concrete(FeatureId(Name("ns1"), "fid3"), BooleanEncoding, Some(CategoricalType), "desc", Nil)))
    }

}
