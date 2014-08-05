package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.storage.fact.{FactsetVersionTwo, FactsetVersion, FactsetVersionOne}

import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.poacher.hdfs._
import com.ambiata.ivory.scoobi._
import WireFormats._
import FactFormats._
import SeqSchemas._

import scala.collection.JavaConverters._
import java.net.URI

object PartitionFactThriftStorageV1 {

  def parseFact(partition: String, tfact: ThriftFact): ParseError \/ Fact =
    parseFactWith(partition, tfact, (_: FactsetId, f: Fact) => f.right)

  val parsePartition: String => ParseError \/ Partition = scalaz.Memo.mutableHashMapMemo { partition: String =>
    Partition.parseWith(new URI(partition).toString).leftMap(ParseError.withLine(new URI(partition).toString)).disjunction
  }

  def parseFactWith[A](partition: String, tfact: ThriftFact, f: (FactsetId, Fact) => ParseError \/ A): ParseError \/ A =
    parsePartition(partition).flatMap(p => parseFactWith(p, tfact, f))

  def parseFact(partition: Partition)(tfact: ThriftFact): ParseError \/ Fact =
    parseFactWith(partition, tfact, (_: FactsetId, f: Fact) => f.right)

  def parseFactWith[A](partition: Partition, tfact: ThriftFact, f: (FactsetId, Fact) => ParseError \/ A): ParseError \/ A =
    f(partition.factset, FatThriftFact(partition.namespace, partition.date, tfact))

  def loadScoobiWith[A : WireFormat](paths: List[String], f: (FactsetId, Fact) => ParseError \/ A, from: Option[Date] = None, to: Option[Date] = None)(implicit sc: ScoobiConfiguration): DList[ParseError \/ A] = {
    val filtered = PartitionExpansion.filterGlob(paths, from, to).toSeq
    if(filtered.nonEmpty)
      valueFromSequenceFileWithPaths[ThriftFact](filtered.toSeq).map({ case (partition, tfact) => parseFactWith(partition, tfact, f) })
    else
      DList[ParseError \/ A]()
  }

  case class PartitionedFactThriftLoader(paths: List[String], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      loadScoobiWith(paths.map(_+"/*/*/*/*"), (_, fact) => fact.right, from, to)
  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[PrioritizedFactset], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[(Priority, FactsetId, Fact)] {
    lazy val factsetMap: Map[FactsetId, Priority] = factsets.map(fs => (fs.set, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ (Priority, FactsetId, Fact)] = {
      loadScoobiWith(factsets.map(base + "/" + _.set.name + "/*/*/*/*"), (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(ParseError(s"FactsetId '${fs}' not found in expected list '${factsets}'", TextError(fs.name)).left), from, to)
    }
  }

  case class PartitionedFactThriftStorer(base: String, codec: Option[CompressionCodec]) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] = {
      val partitioned = dlist.by(f => Partition.path(f.namespace, f.date))
                             .mapValues((f: Fact) => f.toThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      codec.map(partitioned.compressWith(_)).getOrElse(partitioned)
    }
  }
}

object PartitionFactThriftStorageV2 {

  def parseFact(partition: String, tfact: ThriftFact): ParseError \/ Fact =
    parseFactWith(partition, tfact, (_: FactsetId, f: Fact) => f.right)

  val parsePartition: String => ParseError \/ Partition = scalaz.Memo.mutableHashMapMemo { partition: String =>
    Partition.parseWith(new URI(partition).toString).leftMap(ParseError.withLine(new URI(partition).toString)).disjunction
  }

  def parseFactWith[A](partition: String, tfact: ThriftFact, f: (FactsetId, Fact) => ParseError \/ A): ParseError \/ A =
    parsePartition(partition).flatMap(p => parseFactWith(p, tfact, f))

  def parseFact(partition: Partition)(tfact: ThriftFact): ParseError \/ Fact =
    parseFactWith(partition, tfact, (_: FactsetId, f: Fact) => f.right)

  def parseFactWith[A](partition: Partition, tfact: ThriftFact, f: (FactsetId, Fact) => ParseError \/ A): ParseError \/ A =
    f(partition.factset, FatThriftFact(partition.namespace, partition.date, tfact))

  def loadScoobiWith[A : WireFormat](paths: List[String], f: (FactsetId, Fact) => ParseError \/ A, from: Option[Date] = None, to: Option[Date] = None)(implicit sc: ScoobiConfiguration): DList[ParseError \/ A] = {
    val filtered = PartitionExpansion.filterGlob(paths, from, to).toSeq
    if(filtered.nonEmpty)
      valueFromSequenceFileWithPaths[ThriftFact](filtered.toSeq).map({ case (partition, tfact) => parseFactWith(partition, tfact, f) })
    else
      DList[ParseError \/ A]()
  }

  case class PartitionedFactThriftLoader(paths: List[String], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] =
      loadScoobiWith(paths.map(_+"/*/*/*/*"), (_, fact) => fact.right, from, to)

  }

  case class PartitionedMultiFactsetThriftLoader(base: String, factsets: List[PrioritizedFactset], from: Option[Date] = None, to: Option[Date] = None) extends IvoryScoobiLoader[(Priority, FactsetId, Fact)] {
    lazy val factsetMap: Map[FactsetId, Priority] = factsets.map(fs => (fs.set, fs.priority)).toMap

    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ (Priority, FactsetId, Fact)] = {
      loadScoobiWith(factsets.map(base + "/" + _.set.name + "/*/*/*/*"), (fs, fact) =>
        factsetMap.get(fs).map(pri => (pri, fs, fact).right).getOrElse(ParseError(s"FactsetId '${fs}' not found in expected list '${factsets}'", TextError(fs.name)).left), from, to)
    }
  }

  val partitionPath: ((String, Date)) => String = scalaz.Memo.mutableHashMapMemo { nsd =>
    Partition.path(nsd._1, nsd._2)
  }

  case class PartitionedFactThriftStorer(base: String, codec: Option[CompressionCodec]) extends IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] = {
      val partitioned = dlist.by(f => partitionPath((f.namespace, f.date)))
                             .mapValues((f: Fact) => f.toThrift)
                             .valueToPartitionedSequenceFile[PartitionKey, ThriftFact](base, identity, overwrite = true)
      codec.map(partitioned.compressWith(_)).getOrElse(partitioned)
    }
  }
}

object PartitionFactThriftStorage {
  def parseThriftFact(factsetVersion: FactsetVersion, path: String)(tfact: ThriftFact): ParseError \/ Fact =
    factsetVersion match {
      case FactsetVersionOne =>
        import PartitionFactThriftStorageV1._
        for {
          partition <- parsePartition(path)
          fact      <- parseFact(partition)(tfact)
        } yield fact

      case FactsetVersionTwo =>
        import PartitionFactThriftStorageV2._
        for {
          partition <- parsePartition(path)
          fact      <- parseFact(partition)(tfact)
        } yield fact
    }
}

// FIX delete this, should be using storage.fact.Partitions and/or storage.fact.StoreGlob
object PartitionExpansion {
  def filterGlob(paths: List[String], from: Option[Date], to: Option[Date])(implicit sc: ScoobiConfiguration): List[String] =
    (from, to) match {
      case (None, None)         => paths.map(_+"/*")
      case (None, Some(td))     => filterGlobWith(paths, p => Partitions.pathsBeforeOrEqual(p, td))
      case (Some(fd), None)     => filterGlobWith(paths, p => Partitions.pathsAfterOrEqual(p, fd))
      case (Some(fd), Some(td)) => filterGlobWith(paths, p => Partitions.pathsBetween(p, fd, td))
    }

  def filterGlobWith(paths: List[String], f: List[Partition] => List[Partition])(implicit sc: ScoobiConfiguration): List[String] = (for {
    expanded   <- paths.traverse(sp => { val p = new Path(sp); Hdfs.globPaths(p.getParent, p.getName) }).map(_.flatten)
    partitions <- expanded.map(p => Partition.parseDir(p.toString)).collect({ case Success(p) => Hdfs.value(p)}).sequence
  } yield f(partitions)).run(sc).run.unsafePerformIO() match {
    case Error(e) => sys.error(s"Could not access hdfs - ${e}")
    case Ok(glob) => glob.map(_.path)
  }
}
