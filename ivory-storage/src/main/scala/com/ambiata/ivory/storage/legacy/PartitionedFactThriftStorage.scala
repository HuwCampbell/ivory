package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.storage.fact.{FactsetVersionTwo, FactsetVersion, FactsetVersionOne}

import scalaz.{Name => _, DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.CompressionCodec
import com.ambiata.mundane.store._

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.fact.FactsetGlob
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.scoobi._
import FactFormats._


trait PartitionFactThriftStorage {
  val parsePartition: String => ParseError \/ Partition = scalaz.Memo.mutableHashMapMemo { path: String =>
    Partition.parseFile(path).leftMap(ParseError.withLine(path)).disjunction
  }

  def parseFact(path: String, tfact: ThriftFact): ParseError \/ Fact =
    parsePartition(path).map(p => createFact(p, tfact))

  def createFact(partition: Partition, tfact: ThriftFact): Fact =
    FatThriftFact(partition.namespace.name, partition.date, tfact)

  def loadScoobiWith(repo: HdfsRepository, factset: FactsetId, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ Fact]] = for {
    glob  <- ScoobiAction.fromResultTIO((from, to) match {
               case (Some(f), Some(t)) => FactsetGlob.between(repo, factset, f, t)
               case (Some(f), None)    => FactsetGlob.after(repo, factset, f)
               case (None, Some(t))    => FactsetGlob.before(repo, factset, t)
               case (None, None)       => FactsetGlob.select(repo, factset)
             })
    dlist <- loadScoobiFromPaths(glob.map(_.keys.map(k => repo.toIvoryLocation(k).toHdfs)).getOrElse(Nil))
  } yield dlist

  def loadScoobiFromPaths(paths: List[String]): ScoobiAction[DList[ParseError \/ Fact]] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      if(paths.nonEmpty)
        valueFromSequenceFileWithPaths[ThriftFact](paths.toSeq).map({ case (path, tfact) => parseFact(path, tfact) })
      else
        DList[ParseError \/ Fact]()
    })

  case class PartitionedFactThriftLoader(repo: HdfsRepository, factset: FactsetId, from: Option[Date] = None, to: Option[Date] = None) {
    def loadScoobi: ScoobiAction[DList[ParseError \/ Fact]] =
      loadScoobiWith(repo, factset, from, to)
  }

  case class PartitionedMultiFactsetThriftLoader(repo: HdfsRepository, factsets: List[Prioritized[FactsetId]], from: Option[Date] = None, to: Option[Date] = None) {
    def loadScoobi: ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
      factsets.traverseU(pfs =>
        loadScoobiWith(repo, pfs.value, from, to).map(_.map(_.map((pfs.priority, pfs.value, _))))
      ).map(_.foldLeft(DList[ParseError \/ (Priority, FactsetId, Fact)]())(_ ++ _))
  }

  val partitionPath: ((String, Date)) => String = scalaz.Memo.mutableHashMapMemo { nsd =>
    Partition.stringPath(nsd._1, nsd._2)
  }

  case class PartitionedFactThriftStorer(repository: HdfsRepository, key: Key, codec: Option[CompressionCodec]) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
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
  def parseThriftFact(factsetVersion: FactsetVersion, path: String)(tfact: ThriftFact): ParseError \/ Fact =
    factsetVersion match {
      case FactsetVersionOne => PartitionFactThriftStorageV1.parseFact(path, tfact)
      case FactsetVersionTwo => PartitionFactThriftStorageV2.parseFact(path, tfact)
    }
}
