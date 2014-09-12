package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._

import scalaz.Scalaz._

object Extraction {

  def extract(formats: OutputFormats, input: ReferenceIO): IvoryTIO[Unit] = IvoryT.fromResultTIO(repository =>
    formats.outputs.traverse {
      case (PivotFormat, output) =>
        println(s"Pivoting extracted file from '$input' to '${output.path}'")
        Pivot.createPivot(repository, input, output, formats.delim, formats.tombstone)
    }.void
  )
}
