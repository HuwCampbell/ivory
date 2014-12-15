package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftParseError
import com.ambiata.mundane.io.{IOActions, IOAction, Logger}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._, effect._

/**
 * Read a ParseError sequence file and print it to screen
 */
object PrintErrors {

  def print(paths: List[Path], config: Configuration, delim: String): IOAction[Unit] = for {
    l <- IOActions.ask
    _ <- IOActions.fromResultT(Print.printPathsWith(paths, config, new ThriftParseError, printParseError(delim, l)))
  } yield ()

  def printParseError(delim: String, logger: Logger)(path: Path, thrift: ThriftParseError): IO[Unit] = {
    val p = ParseError.fromThrift(thrift)
    val logged = p.data match {
      case TextError(line) => Seq(line, p.message).mkString(delim)
      case _: ThriftError  => p.message
    }
    logger(logged)
  }
}
