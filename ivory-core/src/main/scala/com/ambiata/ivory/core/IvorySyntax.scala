package com.ambiata.ivory.core

import com.ambiata.ivory.data.DataSyntax
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.commons.logging.Log
import org.apache.hadoop.fs.Path

import scalaz.effect.IO

trait IvorySyntax extends DataSyntax {
  implicit class IvoryFilePathSyntax(f: FilePath) {
    def toHdfs: Path = new Path(f.path)

    // TODO move to FilePath
    def components: List[String] =
      f.path.split("/").toList
    def drop(n: Int): Option[FilePath] =
      (1 to n).toList.foldLeft(f.parent)((acc, _) => acc.flatMap(_.parent))
  }

  def checkThat[A](a: =>A, condition: Boolean, message: String): ResultTIO[A] =
    if (condition) ResultT.safe[IO, A](a)
    else           ResultT.fail[IO, A](message)

  /**
   * Logging utility functions when working with ResultTIO for now
   * This will be removed when IvoryT is introduced
   */

  def logDebug(message: String)(implicit logger: Log): ResultTIO[Unit] =
    ResultT.ok[IO, Unit](logger.debug(message))

  def logInfo(message: String)(implicit logger: Log): ResultTIO[Unit] =
    ResultT.ok[IO, Unit](logger.info(message))

  implicit class Logged[T](result: ResultTIO[T])(implicit logger: Log) {

    def timed(message: String): ResultTIO[T] = for {
      start <- ResultT.ok[IO, Long](System.currentTimeMillis)
      r     <- result
      end   <- ResultT.ok[IO, Long](System.currentTimeMillis)
      _     <- logDebug(message+s" in ${end - start}ms")
    } yield r

    def debug(message: String): ResultTIO[T] = for {
      r <- result
      _ <- logDebug(message)
    } yield r

    def info(message: String): ResultTIO[T] = for {
      r <- result
      _ <- logInfo(message)
    } yield r
  }
}

object IvorySyntax extends IvorySyntax
