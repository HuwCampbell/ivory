package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.storage.fact.FactsetVersion
import com.ambiata.ivory.storage.legacy._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import org.apache.hadoop.fs.{Hdfs => _, _}
import org.apache.hadoop.conf.Configuration
import scalaz.{Value => _, _}, Scalaz._, effect._

/**
 * Read a facts sequence file and print it to screen
 */
object PrintFacts {

  def print(paths: List[Path], config: Configuration, delim: Char, tombstone: String, version: FactsetVersion): IOAction[Unit] = for {
    logger           <- IOActions.ask
    isPartitionPath  <- paths.traverseU(isPartition(config)).map(_.contains(true))
    _                <- IOActions.fromResultT(
      if (isPartitionPath)
        Print.printPathsWith(paths, config, new ThriftFact, printThriftFact(delim, tombstone, logger, version))
      else
        Print.printPathsWith(paths, config, createMutableFact, printFact(delim, tombstone, logger))
    )
  } yield ()

  private def printThriftFact(delim: Char, tombstone: String, logger: Logger, version: FactsetVersion)(path: Path, tf: ThriftFact): IO[Unit] =
    PartitionFactThriftStorage.parseThriftFact(version, path.toString)(tf) match {
      case -\/(e) => logger("Error! "+e.message+ (e.data match {
        case TextError(line) => ": " + line
        case _: ThriftError  => ""
      }))
      case \/-(f) => printFact(delim, tombstone, logger)(path, f)
    }

  private def printFact(delim: Char, tombstone: String, logger: Logger)(path: Path, f: Fact): IO[Unit] = {
     val logged =
       Seq(f.entity,
         f.namespace.name,
         f.feature,
         TextEscaping.escape(delim, Value.toStringWithStruct(f.value, tombstone)),
         f.date.hyphenated+delim+f.time.hhmmss).mkString(delim.toString)

     logger(logged)
  }

  /** @return true if the path is a partition */
  private def isPartition(config: Configuration)(path: Path): IOAction[Boolean] = {
    val (basePath, glob) = Hdfs.pathAndGlob(path)
    IOActions.fromResultT(Hdfs.globFiles(basePath, glob).map(_.exists(p => Partition.parseFile(FilePath.unsafe(p.toUri.toString)).toOption.isDefined)).run(config))
  }

}
