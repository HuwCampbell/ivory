package com.ambiata.ivory.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io.DirPath
import org.apache.hadoop.fs.Path

trait IvorySyntax {
  def checkThat[A](a: => A, condition: Boolean, message: String): RIO[A] =
    if (condition) RIO.safe[A](a)
    else           RIO.fail[A](message)

  implicit class PathToIvoryDirPathSyntax(path: Path) {
    def toDirPath: DirPath = DirPath.unsafe(path.toString)
  }
}

object IvorySyntax extends IvorySyntax
