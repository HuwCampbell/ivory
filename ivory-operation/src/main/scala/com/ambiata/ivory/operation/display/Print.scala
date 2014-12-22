package com.ambiata.ivory.operation.display

import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.poacher.mr.{ThriftLike, ThriftSerialiser}
import org.apache.hadoop.io.{BytesWritable, NullWritable, SequenceFile}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._, effect._, effect.Effect._

/**
 * Read a facts sequence file and print it to screen
 */
object Print {

  def printPathsWith[A](paths: List[Path], configuration: Configuration, thrift: A, printA: (Path, A) => IO[Unit])(implicit ev: A <:< ThriftLike): RIO[Unit] =
    paths.traverseU(path => for {
      files       <- {
        val (basePath, glob) = Hdfs.pathAndGlob(path)
        Hdfs.globFiles(basePath, glob).filterHidden.run(configuration)
      }
      _ <- files.traverse(file => printWith(file, configuration, thrift, printA))
    } yield ()).void

  def printWith[A](path: Path, configuration: Configuration, thrift: A, printA: (Path, A) => IO[Unit])(implicit ev: A <:< ThriftLike): RIO[Unit] =
    ResultT.using(ResultT.io(new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path)))) { reader => ResultT.io {
      val bytes = new BytesWritable()
      val serialiser = ThriftSerialiser()
      while (reader.next(NullWritable.get, bytes)) {
        serialiser.fromBytesViewUnsafe(thrift, bytes.getBytes, 0, bytes.getLength)
        printA(path, thrift).unsafePerformIO
      }
    }}
}
