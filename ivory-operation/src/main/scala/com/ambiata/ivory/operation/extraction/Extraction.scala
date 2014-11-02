package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.operation.extraction.output._

import scalaz.Scalaz._

object Extraction {

  def extract(formats: OutputFormats, input: IvoryLocation, dictionary: Dictionary): IvoryTIO[Unit] = IvoryT.fromResultTIO(repository =>
    formats.outputs.traverse {
      case (DenseFormat(delim), output) =>
        println(s"Storing extracted data '$input' to '${output.show}'")
        PivotOutput.createPivotWithDictionary(repository, input, output, dictionary, delim, formats.missingValue)
      case (SparseFormat(delim), output) =>
        println(s"Storing extracted data '$input' to '${output.show}'")
        EavOutput.extractWithDictionary(repository, input, output, dictionary, delim, formats.missingValue)
    }.void
  )
}
