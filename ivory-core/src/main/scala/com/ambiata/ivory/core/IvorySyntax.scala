package com.ambiata.ivory.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.DirPath
import org.apache.commons.logging.Log
import org.apache.hadoop.fs.Path
import scalaz.effect.IO

trait IvorySyntax {

  def checkThat[A](a: =>A, condition: Boolean, message: String): ResultTIO[A] =
    if (condition) ResultT.safe[IO, A](a)
    else           ResultT.fail[IO, A](message)

  /**
   * try to cast an object to a given subtype
   * short-term solution for working on HdfsRepositories only
   */
  def downcast[A, B <: A](a: A, message: String): ResultTIO[B] =
    ResultT.safe[IO, A](a).flatMap { a1 =>
      try ResultT.ok[IO, B](a.asInstanceOf[B])
      catch { case _:Exception => ResultT.fail[IO, B](message) }
    }

  implicit class PathToIvoryDirPathSyntax(path: Path) {
    def toDirPath: DirPath = DirPath.unsafe(path.toString)
  }
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
