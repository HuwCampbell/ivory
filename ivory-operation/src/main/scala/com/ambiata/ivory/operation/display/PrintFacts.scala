package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.storage.legacy._
import com.ambiata.mundane.control.RIO
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import org.apache.hadoop.fs.{Hdfs => _, _}
import org.apache.hadoop.conf.Configuration
import scalaz.{Value => _, _}, Scalaz._, effect._

/**
 * Read a facts sequence file and print it to screen
 */
object PrintFacts {

  def print(paths: List[Path], config: Configuration, delim: Char, tombstone: String, version: FactsetFormat): RIO[Unit] = for {
    isPartitionPath  <- paths.traverseU(isPartition(config)).map(_.contains(true))
    _                <-
      if (isPartitionPath)
        Print.printPathsWith(paths, config, new ThriftFact)(printThriftFact(delim, tombstone, version))
      else
        Print.printPathsWith(paths, config, createMutableFact)((_, f) => printFact(delim, tombstone)(f))
  } yield ()

  def printThriftFact(delim: Char, tombstone: String, version: FactsetFormat)(path: Path, tf: ThriftFact): IO[Unit] =
    PartitionFactThriftStorage.parseThriftFact(version, path.toString)(tf) match {
      case -\/(e) => IO.putStrLn("Error! "+e.message+ (e.data match {
        case TextError(line) => ": " + line
        case _: ThriftError  => ""
      }))
      case \/-(f) => printFact(delim, tombstone)(f)
    }

  def renderFact(delim: Char, tombstone: String, f: Fact): String =
    Seq(f.entity,
        f.namespace.name,
        f.feature,
        TextEscaping.escape(delim, Value.toStringWithStruct(f.value, tombstone)),
        f.date.hyphenated+delim+f.time.hhmmss).mkString(delim.toString)

  def printFact(delim: Char, tombstone: String)(f: Fact): IO[Unit] =
    IO.putStrLn(renderFact(delim, tombstone, f))

  /** @return true if the path is a partition */
   def isPartition(config: Configuration)(path: Path): RIO[Boolean] = {
    val (basePath, glob) = Hdfs.pathAndGlob(path)
    Hdfs.globFiles(basePath, glob).map(_.exists(p => Partition.parseFile(FilePath.unsafe(p.toUri.toString)).toOption.isDefined)).run(config)
  }

}
