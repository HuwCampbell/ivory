package com.ambiata.ivory.tools

import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.storage.fact.FactsetVersion
import com.ambiata.ivory.storage.legacy.{PartitionFactThriftStorage, PartitionFactThriftStorageV2}

import scalaz.{\/-, -\/}
import scalaz.concurrent.Task
import scalaz.syntax.traverse._
import scalaz.std.list._
import org.apache.hadoop.io.{ByteWritable, SequenceFile, BytesWritable, NullWritable}
import org.apache.avro.mapred.SequenceFileReader
import scalaz.stream.Process
import scalaz.stream.Process.End
import com.ambiata.ivory.core._
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import scalaz.std.anyVal._
import com.ambiata.ivory.scoobi.{ScoobiAction, SeqSchemas}
import org.apache.hadoop.fs.{Path}
import org.apache.hadoop.conf.Configuration
import IOActions._

/**
 * Read a facts sequence file and print it to screen
 */
object PrintFacts {

  def print(paths: List[Path], config: Configuration, delim: String, tombstone: String, version: FactsetVersion): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printPathsWith(paths, config, SeqSchemas.thriftFactSeqSchema, printFact(delim, tombstone, l, version))
  } yield ()

  def printFact(delim: String, tombstone: String, logger: Logger, version: FactsetVersion)(path: Path, tf: ThriftFact): Task[Unit] = Task.delay {
    PartitionFactThriftStorage.parseThriftFact(version, path.toString)(tf) match {
      case -\/(e) =>
        logger("Error! "+e).unsafePerformIO

      case \/-(f) =>
        val logged =
          Seq(f.entity,
            f.namespace,
            f.feature,
            if(f.isTombstone) tombstone else f.value.stringValue.getOrElse(""),
            f.date.hyphenated+delim+f.time.hhmmss).mkString(delim)

        logger(logged).unsafePerformIO
    }

  }
}
