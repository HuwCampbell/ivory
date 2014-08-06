package com.ambiata.ivory.operation.display

import scalaz.concurrent.Task
import com.ambiata.ivory.core._
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import scalaz.std.anyVal._
import com.ambiata.ivory.scoobi.SeqSchemas
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import com.ambiata.poacher.hdfs.Hdfs

/**
 * Read a ParseError sequence file and print it to screen
 */
object PrintErrors {

  def print(paths: List[Path], config: Configuration, delim: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- Print.printPathsWith(paths, config, SeqSchemas.parseErrorSeqSchema, printParseError(delim, l))
  } yield ()

  def printParseError(delim: String, logger: Logger)(path: Path, p: ParseError): Task[Unit] = Task.delay {
    val logged = p.data match {
      case TextError(line) => Seq(line, p.message).mkString(delim)
      case _: ThriftError  => p.message
    }
    logger(logged).unsafePerformIO()
  }
}
