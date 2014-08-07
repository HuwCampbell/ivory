package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import org.apache.hadoop.fs.Path

trait IvorySyntax {
  implicit class IvoryFilePathSyntax(f: FilePath) {
    def toHdfs: Path = new Path(f.path)

    // TODO move to FilePath
    def components: List[String] =
      f.path.split("/").toList
    def drop(n: Int): Option[FilePath] =
      (1 to n).toList.foldLeft(f.parent)((acc, _) => acc.flatMap(_.parent))
  }
}

object IvorySyntax extends IvorySyntax
