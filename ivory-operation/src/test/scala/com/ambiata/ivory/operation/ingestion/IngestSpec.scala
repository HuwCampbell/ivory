package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.ClusterTemporary._
import com.ambiata.ivory.core.IvoryLocationTemporary._
import com.ambiata.ivory.core.RepositoryTemporary._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.operation.ingestion.thrift.Conversion
import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.metadata.{DictionaryThriftStorage, FeatureIdMappingsStorage}
import com.ambiata.ivory.storage.parse.EavtParsers
import com.ambiata.ivory.storage.repository.{HdfsGlobs, Repositories}
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import com.ambiata.poacher.mr.ThriftSerialiser
import com.nicta.scoobi.Scoobi._
import org.specs2.{ScalaCheck, Specification}
import scalaz.{Value=>_,_}, Scalaz._
import MemoryConversions._

class IngestSpec extends Specification with ScalaCheck { def is = section("mr") ^ section("aws") ^ s2"""

 Facts can be ingested from
   a directory named namespace/year/month/day containing fact files                       $partitionIngest
   a directory containing fact files (and the namespace is specified on the command line) $namespaceIngest

   from escaped delimited text                                                            $escapedText
   from thrift format                                                                     $thrift

 FeatureId Mappings are stored when facts ingested                                        $featureIdMappings
"""

  def partitionIngest = prop((facts: SparseEntities, tt: TemporaryType) => {
    withRepository(Hdfs) { repository: Repository =>
      withCluster { cluster: Cluster =>
        withIvoryLocationDir(tt) { location =>
          Repositories.create(repository, RepositoryConfig.testing) >>
          DictionaryThriftStorage(repository).store(facts.dictionary) >>
          IvoryLocation.writeUtf8Lines(location </> FileName.unsafe(facts.fact.featureId.namespace.name) </> "part-r-00000", List(facts.fact).map(toEavt)) >>
          IvoryLocation.writeUtf8Lines(location </> FileName.unsafe(facts.fact.featureId.namespace.name) </> "part-r-00001", List(facts.fact).map(toEavt)) >>
          Ingest.ingestFacts(repository, cluster, List(
            (FileFormat.Text(Delimiter.Psv, TextEscaping.Delimited, TextFormat.Json), None, location)
          ), None, 100.mb).run.run(IvoryRead.create)
        }
      }
    } must beOk
  }).set(minTestsOk = 5)

  def namespaceIngest = prop((facts: SparseEntities, tt: TemporaryType) => {
    withRepository(Hdfs) { repository: Repository =>
      withCluster { cluster: Cluster =>
        withIvoryLocationDir(tt) { location =>
          Repositories.create(repository, RepositoryConfig.testing) >>
          DictionaryThriftStorage(repository).store(facts.dictionary) >>
          IvoryLocation.writeUtf8Lines(location </> "part-r-00000", List(facts.fact).map(toEavt)) >>
          IvoryLocation.writeUtf8Lines(location </> "part-r-00001", List(facts.fact).map(toEavt)) >>
          Ingest.ingestFacts(repository, cluster, List(
            (FileFormat.Text(Delimiter.Psv, TextEscaping.Delimited, TextFormat.Json), Some(facts.fact.namespace), location)
          ), None, 100.mb).run.run(IvoryRead.create)
        }
      }
    } must beOk
  }).set(minTestsOk = 5)

  def escapedText = prop((facts: SparseEntities, tt: TemporaryType) => {
    withRepository(Hdfs) { repository: Repository =>
      withCluster { cluster: Cluster =>
        withIvoryLocationDir(tt) { location =>
          for {
            _  <- Repositories.create(repository, RepositoryConfig.testing)
            _  <- DictionaryThriftStorage(repository).store(facts.dictionary)
            _  <- IvoryLocation.writeUtf8Lines(location </> "part-r-00000", List(facts.fact).map(toEavtEscaped))
            _  <- Ingest.ingestFacts(repository, cluster, List(
              (FileFormat.Text(Delimiter.Psv, TextEscaping.Escaped, TextFormat.Json), Some(facts.fact.namespace), location)
            ), None, 100.mb).run.run(IvoryRead.create)
            r  <- repository.asHdfsRepository
            l  <- repository.toIvoryLocation(Repository.namespace(FactsetId.initial, facts.fact.namespace)).asHdfsIvoryLocation
          } yield List(facts.fact.toThrift) -> valueFromSequenceFile[ThriftFact](l.toHdfs + "/" + HdfsGlobs.FactsetPartitionsGlob).run(r.scoobiConfiguration).toList
        }
      }
    } must beOkLike(f => f._1 ==== f._2)
  }).set(minTestsOk = 2)

  def thrift = prop {(facts: FactsWithDictionary, fact: Fact, tt: TemporaryType) =>
    val serialiser = ThriftSerialiser()
    val ns = facts.cg.fid.namespace
    //  Lazy, but guaranteed to be bad so we always have at least one error
    val badFacts = List(fact.withFeatureId(facts.cg.fid).withValue(StructValue(Map("" -> StringValue("")))))
    withCluster { cluster: Cluster =>
      withIvoryLocationDir(tt) { loc =>
        withHdfsRepository { repository => for {
          _   <- Repositories.create(repository, RepositoryConfig.testing)
          c   = cluster.hdfsConfiguration
          _   <- DictionaryThriftStorage(repository).store(facts.dictionary)
          _   <- SequenceUtil.writeBytes((loc </> "part-r-00000").location, c, cluster.s3Client, None) {
            write =>
              RIO.safe((facts.facts ++ badFacts).foreach(fact => write(serialiser.toBytes(Conversion.fact2thrift(fact))))) >>
              RIO.safe(write(serialiser.toBytes(Conversion.fact2thrift(fact).setDatetime("bad-date"))))
          }
          fid <- Ingest.ingestFacts(repository, cluster, List(
            (FileFormat.Thrift, Some(ns), loc)
          ), None, 100.mb).run.run(IvoryRead.create)
        } yield (
          valueFromSequenceFile[ThriftFact](repository.toIvoryLocation(Repository.namespace(fid, ns)).toHdfs + "/*/*/*/*").run(repository.scoobiConfiguration).toSet,
          valueFromSequenceFile[ThriftFact](repository.toIvoryLocation(Repository.errors).toHdfs + "/*/*").run(repository.scoobiConfiguration).size
        )
        }
      }
    } must beOkValue(facts.facts.map(_.toThrift).toSet -> (badFacts.size + 1))
  }.set(minTestsOk = 3, maxDiscardRatio = 10)

  def featureIdMappings = prop((facts: SparseEntities, tlocation: IvoryLocationTemporary, rtemp: RepositoryTemporary) => {
    val expected: List[FeatureId] = FeatureIdMappings.fromDictionary(facts.dictionary).featureIds
    (for {
      repository <- rtemp.hdfs
      cluster    <- ClusterTemporary().cluster
      location   <- tlocation.file
      _          <- Repositories.create(repository, RepositoryConfig.testing)
      _          <- DictionaryThriftStorage(repository).store(facts.dictionary)
      _          <- IvoryLocation.writeUtf8Lines(location </> "part-r-00000", List(facts.fact).map(toEavt))
      fid        <- Ingest.ingestFacts(repository, cluster, List(
                      (FileFormat.Text(Delimiter.Psv, TextEscaping.Delimited, TextFormat.Json), Some(facts.fact.namespace), location)
                    ), None, 100.mb).run.run(IvoryRead.create)
      ms         <- FeatureIdMappingsStorage.fromKeyStore(repository, Repository.factset(fid) / FeatureIdMappingsStorage.keyname).map(_.featureIds)
    } yield ms) must beOkValue(expected)
  }).set(minTestsOk = 5)

  def toEavt(fact: Fact) =
    EavtParsers.toEavtDelimited(fact, "NA", '|')

  def toEavtEscaped(fact: Fact) =
    TextEscaping.mkString('|', EavtParsers.toEavt(fact, "NA"))
}
