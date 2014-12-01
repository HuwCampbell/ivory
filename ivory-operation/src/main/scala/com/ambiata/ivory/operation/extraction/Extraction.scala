package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.operation.extraction.output._

import scalaz.Scalaz._

object Extraction {

  def extract(formats: OutputFormats, input: IvoryLocation, dictionary: Dictionary): RepositoryTIO[Unit] = RepositoryT.fromResultTIO(repository =>
    formats.outputs.traverse {
      case (DenseFormat(format), output) =>
        println(s"Storing extracted data '$input' to '${output.show}'")
        GroupByEntityOutput.createWithDictionary(repository, input, output, dictionary, format match {
          case DelimitedFile(delim) => GroupByEntityFormat.DenseText(delim, formats.missingValue, false)
          case EscapedFile(delim)   => GroupByEntityFormat.DenseText(delim, formats.missingValue, true)
          case ThriftFile           => GroupByEntityFormat.DenseThrift
        })
      case (SparseFormat(ThriftFile), output) =>
        println(s"Storing extracted data '$input' to '${output.show}'")
        GroupByEntityOutput.createWithDictionary(repository, input, output, dictionary, GroupByEntityFormat.SparseThrift)
      case (SparseFormat(DelimitedFile(delim)), output) =>
        println(s"Storing extracted data '$input' to '${output.show}'")
        SparseOutput.extractWithDictionary(repository, input, output, dictionary, delim, formats.missingValue, false)
      case (SparseFormat(EscapedFile(delim)), output) =>
        println(s"Storing extracted data '$input' to '${output.show}'")
        SparseOutput.extractWithDictionary(repository, input, output, dictionary, delim, formats.missingValue, true)
    }.void
  )
}
