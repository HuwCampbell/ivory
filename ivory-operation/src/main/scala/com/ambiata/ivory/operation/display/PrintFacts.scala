package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftParseError
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.operation.debug.DumpFactsMapper
import com.ambiata.ivory.operation.extraction.IvoryInputs
import com.ambiata.mundane.control.RIO
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr.{Writables, ThriftSerialiser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, NullWritable, IntWritable, Writable}
import scalaz._, Scalaz._, effect._

object PrintFacts {

  def print[K <: Writable, V <: Writable](fact: NamespacedFact, converter: MrFactConverter[K, V], mapper: DumpFactsMapper)(key: K, value: V): IO[Unit] = {
    converter.convert(fact, key, value)
    IO.putStrLn(if(mapper.accept(fact)) mapper.render(fact) else "")
  }

  def printFactset(repository: Repository, factset: Factset, entities: List[String], attributes: List[String]): RIO[String \/ Unit] = {
    val mapper = DumpFactsMapper(entities.toSet, attributes.toSet, s"Factset[${factset.id.render}]")
    repository match {
      case hr @ HdfsRepository(_) => for {
        paths <- IvoryInputs.factsetPaths(hr, factset).traverse(expandPath).map(_.flatten).run(hr.configuration)
        ret   <- paths.traverse(path => {
          val converter = factset.format match {
            case FactsetFormat.V1 => MrFactsetFactFormatV1.factConverter(path)
            case FactsetFormat.V2 => MrFactsetFactFormatV2.factConverter(path)
          }
          Print.printWith(path, hr.configuration, NullWritable.get, Writables.bytesWritable(4096))(print(createNamespacedFact, converter, mapper) _)
        }).map(_.sequenceU.right)
      } yield ret
      case LocalRepository(_) => RIO.ok("DumpFacts does not support reading factsets from local repositories yet".left)
      case S3Repository(_) => RIO.ok("DumpFacts does not support reading factsets from s3 repositories yet".left)
    }
  }

  def printSnapshot(repository: Repository, snapshot: Snapshot, entities: List[String], attributes: List[String]): RIO[String \/ Unit] = {
    val mapper = DumpFactsMapper(entities.toSet, attributes.toSet, s"Snapshot[${snapshot.id.render}]")

    def printPath[K <: Writable](path: Path, conf: Configuration, key: K, converter: MrFactConverter[K, BytesWritable]): RIO[Unit] =
      Print.printWith(path, conf, key, Writables.bytesWritable(4096))(print(createNamespacedFact, converter, mapper) _)

    repository match {
      case hr @ HdfsRepository(_) => for {
        paths <- IvoryInputs.snapshotPaths(hr, snapshot).traverse(expandPath).map(_.flatten).run(hr.configuration)
        ret   <- paths.traverse(path => snapshot.format match {
          case SnapshotFormat.V1 => printPath(path, hr.configuration, NullWritable.get, MrSnapshotFactFormatV1.factConverter(path))
          case SnapshotFormat.V2 => printPath(path, hr.configuration, new IntWritable, MrSnapshotFactFormatV2.factConverter(path))
        }).map(_.sequenceU.right)
      } yield ret
      case LocalRepository(_) => RIO.ok("DumpFacts does not support reading snapshots from local repositories yet".left)
      case S3Repository(_) => RIO.ok("DumpFacts does not support reading snapshots from s3 repositories yet".left)
    }
  }

  def expandPath(path: Path): Hdfs[List[Path]] =
    Hdfs.globPaths(path, "*").filterHidden
}

