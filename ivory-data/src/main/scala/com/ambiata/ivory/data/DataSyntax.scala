package com.ambiata.ivory.data

import com.ambiata.mundane.io._

trait DataSyntax {
  implicit class IvoryFilePathListSyntax(l: List[FilePath]) {
    def filterHidden: List[FilePath] =
      l.filter(f => !f.basename.path.startsWith(".") && !f.basename.path.startsWith("_"))
  }
}

object DataSyntax extends DataSyntax
