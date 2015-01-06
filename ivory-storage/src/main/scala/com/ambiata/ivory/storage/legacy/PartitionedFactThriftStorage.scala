package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.CompressionCodec
import com.ambiata.notion.core._

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.mr._
import FactFormats._


trait PartitionFactThriftStorage {
  val parsePartition: String => ParseError \/ Partition = scalaz.Memo.mutableHashMapMemo { path: String =>
    Partition.parseFile(path).leftMap(ParseError.withLine(path)).disjunction
  }

  def parseFact(path: String, tfact: ThriftFact): ParseError \/ Fact =
    parsePartition(path).map(p => createFact(p, tfact))

  def createFact(partition: Partition, tfact: ThriftFact): Fact =
    FatThriftFact(partition.namespace.name, partition.date, tfact)

  def loadScoobiWith(repo: HdfsRepository, factset: FactsetId): ScoobiAction[DList[ParseError \/ Fact]] =
    loadScoobiFromPaths(List(repo.toIvoryLocation(Repository.factset(factset)).toHdfs))

  def loadScoobiFromPaths(paths: List[String]): ScoobiAction[DList[ParseError \/ Fact]] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      if(paths.nonEmpty)
        valueFromSequenceFileWithPaths[ThriftFact](paths.toSeq).map({ case (path, tfact) => parseFact(path, tfact) })
      else
        DList[ParseError \/ Fact]()
    })

  val partitionPath: ((String, Date)) => String = scalaz.Memo.mutableHashMapMemo { nsd =>
    Partition.stringPath(nsd._1, nsd._2)
  }

  case class PartitionedFactThriftStorer(repository: HdfsRepository, key: Key, codec: Option[CompressionCodec]) {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] = {
      dlist.by(f => partitionPath((f.namespace.name, f.date)))
           .mapValues((f: Fact) => f.toThrift)
           .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](repository.toIvoryLocation(key).toHdfs, identity, overwrite = true).persistWithCodec(codec)
    }
  }
}

object PartitionFactThriftStorageV1 extends PartitionFactThriftStorage
object PartitionFactThriftStorageV2 extends PartitionFactThriftStorage

object PartitionFactThriftStorage {
  def parseThriftFact(factsetVersion: FactsetFormat, path: String)(tfact: ThriftFact): ParseError \/ Fact =
    factsetVersion match {
      case FactsetFormat.V1 => PartitionFactThriftStorageV1.parseFact(path, tfact)
      case FactsetFormat.V2 => PartitionFactThriftStorageV2.parseFact(path, tfact)
    }
}
