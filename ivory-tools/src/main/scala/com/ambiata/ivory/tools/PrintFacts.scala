package com.ambiata.ivory.tools

import scalaz.stream.{Sink, io}
import java.io.File
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
import com.ambiata.ivory.alien.hdfs.Hdfs

/**
 * Read a facts sequence file and print it to screen
 */
object PrintFacts {

  def print(paths: List[Path], config: Configuration, delim: String, tombstone: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printPathsWith(paths, config, SeqSchemas.factSeqSchema, printFact(delim, tombstone, l))
  } yield ()

  def printFact(delim: String, tombstone: String, logger: Logger)(path: Path, f: Fact): Task[Unit] = Task.delay {
    // We're ignoring structs here
    val logged = Value.toString(f.value, Some(tombstone)).map { value =>
      Seq(f.entity,
          f.namespace,
          f.feature,
          value,
          f.date.hyphenated+delim+f.time.hhmmss).mkString(delim)
    }
    logged.foreach(logger(_).unsafePerformIO())
  }
}
