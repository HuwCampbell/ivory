package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.storage.fact.FactsetVersion
import com.ambiata.ivory.storage.legacy._

import scalaz.{\/-, -\/}
import scalaz.concurrent.Task
import com.ambiata.ivory.core.{TextError, ThriftError, Value, Partition, Fact}
import com.ambiata.mundane.io.{IOActions, IOAction, Logger, FilePath}
import scalaz.std.anyVal._
import com.ambiata.ivory.mr._
import org.apache.hadoop.fs.{Hdfs => _, _}
import org.apache.hadoop.conf.Configuration
import scalaz.syntax.traverse._
import scalaz.std.list._
import com.ambiata.poacher.hdfs.Hdfs

/**
 * Read a facts sequence file and print it to screen
 */
object PrintFacts {

  def print(paths: List[Path], config: Configuration, delim: String, tombstone: String, version: FactsetVersion): IOAction[Unit] = for {
    logger           <- IOActions.ask
    isPartitionPath  <- paths.traverseU(isPartition(config)).map(_.contains(true))
    _                <-
      if (isPartitionPath)
        Print.printPathsWith(paths, config, SeqSchemas.thriftFactSeqSchema, printThriftFact(delim, tombstone, logger, version))
      else
        Print.printPathsWith(paths, config, SeqSchemas.factSeqSchema, printFact(delim, tombstone, logger))
  } yield ()

  private def printThriftFact(delim: String, tombstone: String, logger: Logger, version: FactsetVersion)(path: Path, tf: ThriftFact): Task[Unit] = Task.delay {
    PartitionFactThriftStorage.parseThriftFact(version, path.toString)(tf) match {
      case -\/(e) => Task.now(logger("Error! "+e.message+ (e.data match {
        case TextError(line) => ": " + line
        case _: ThriftError  => ""
      })).unsafePerformIO())
      case \/-(f) => printFact(delim, tombstone, logger)(path, f)
    }
  }.flatMap(identity)

  private def printFact(delim: String, tombstone: String, logger: Logger)(path: Path, f: Fact): Task[Unit] = Task.delay {
     val logged =
       Seq(f.entity,
         f.namespace.name,
         f.feature,
         Value.toStringWithStruct(f.value, tombstone),
         f.date.hyphenated+delim+f.time.hhmmss).mkString(delim)

     logger(logged).unsafePerformIO
  }

  /** @return true if the path is a partition */
  private def isPartition(config: Configuration)(path: Path): IOAction[Boolean] = {
    val (basePath, glob) = Hdfs.pathAndGlob(path)
    IOActions.fromResultT(Hdfs.globFiles(basePath, glob).map(_.exists(p => Partition.parseFile(FilePath.unsafe(p.toUri.toString)).toOption.isDefined)).run(config))
  }

}
