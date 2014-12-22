package com.ambiata.ivory.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.DirPath
import org.apache.commons.logging.Log
import org.apache.hadoop.fs.Path
import scalaz.effect.IO

trait IvorySyntax {
  def checkThat[A](a: => A, condition: Boolean, message: String): RIO[A] =
    if (condition) ResultT.safe[IO, A](a)
    else           ResultT.fail[IO, A](message)

  implicit class PathToIvoryDirPathSyntax(path: Path) {
    def toDirPath: DirPath = DirPath.unsafe(path.toString)
  }
}

object IvorySyntax extends IvorySyntax
