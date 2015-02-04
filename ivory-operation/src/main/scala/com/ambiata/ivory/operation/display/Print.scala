package com.ambiata.ivory.operation.display

import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs.Hdfs
import org.apache.hadoop.io.{Writable, SequenceFile}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._, effect._, effect.Effect._

/**
 * Read a facts sequence file and print it to screen
 */
object Print {

  def printPathsWith[K <: Writable, V <: Writable](paths: List[Path], configuration: Configuration, key: K, value: V)(printKV: (K, V) => IO[Unit]): RIO[Unit] =
    paths.traverseU(path => for {
      files       <- {
        val (basePath, glob) = Hdfs.pathAndGlob(path)
        Hdfs.globFiles(basePath, glob).filterHidden.run(configuration)
      }
      _ <- files.traverse(file => printWith(file, configuration, key, value)(printKV))
    } yield ()).void

  def printWith[K <: Writable, V <: Writable](path: Path, configuration: Configuration, key: K, value: V)(printKV: (K, V) => IO[Unit]): RIO[Unit] =
    RIO.using(RIO.io(new SequenceFile.Reader(configuration, SequenceFile.Reader.file(path))))(reader => RIO.io {
      while (reader.next(key, value))
        printKV(key, value).unsafePerformIO
    })
}
