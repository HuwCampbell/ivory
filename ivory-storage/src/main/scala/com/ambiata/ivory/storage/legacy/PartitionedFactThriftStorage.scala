package com.ambiata.ivory.storage.legacy

import scalaz.{DList => _, Value => _, _}
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.repository._
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.mr._
import FactFormats._


object PartitionFactThriftStorage {
  val parsePartition: String => ParseError \/ Partition = scalaz.Memo.mutableHashMapMemo { path: String =>
    Partition.parseFile(path).leftMap(ParseError.withLine(path)).disjunction
  }

  def parseFact(path: String, tfact: ThriftFact): ParseError \/ Fact =
    parsePartition(path).map(p => createFact(p, tfact))

  def createFact(partition: Partition, tfact: ThriftFact): Fact =
    FatThriftFact(partition.namespace.name, partition.date, tfact)

  def loadScoobiWith(repo: HdfsRepository, factset: FactsetId): ScoobiAction[DList[ParseError \/ Fact]] =
    loadScoobiFromPaths(List(new Path(repo.toIvoryLocation(Repository.factset(factset)).toHdfs, HdfsGlobs.FactsetPartitionsGlob).toString))

  def loadScoobiFromPaths(paths: List[String]): ScoobiAction[DList[ParseError \/ Fact]] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      if(paths.nonEmpty)
        valueFromSequenceFileWithPaths[ThriftFact](paths.toSeq).map({ case (path, tfact) => parseFact(path, tfact) })
      else
        DList[ParseError \/ Fact]()
    })

  def parseThriftFact(factsetVersion: FactsetFormat, path: String)(tfact: ThriftFact): ParseError \/ Fact =
    factsetVersion match {
      case FactsetFormat.V1 | FactsetFormat.V2  => parseFact(path, tfact)
    }
}
