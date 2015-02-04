package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftParseError
import com.ambiata.mundane.control.RIO
import com.ambiata.poacher.mr.{Writables, ThriftSerialiser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import scalaz._, effect._

/**
 * Read a ParseError sequence file and print it to screen
 */
object PrintErrors {

  def print(paths: List[Path], config: Configuration, delim: String): RIO[Unit] =
    Print.printPathsWith(paths, config, NullWritable.get, Writables.bytesWritable(4096))(printParseError(delim, new ThriftParseError, ThriftSerialiser()) _)

  def printParseError(delim: String, thrift: ThriftParseError, serialiser: ThriftSerialiser)(key: NullWritable, value: BytesWritable): IO[Unit] = {
    serialiser.fromBytesViewUnsafe(thrift, value.getBytes, 0, value.getLength)
    val p = ParseError.fromThrift(thrift)
    val logged = p.data match {
      case TextError(line) => Seq(line, p.message).mkString(delim)
      case _: ThriftError  => p.message
    }
    IO.putStrLn(logged)
  }
}
