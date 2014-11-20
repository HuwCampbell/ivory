package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.operation.extraction.output._

import scalaz.Scalaz._

object Extraction {

/*
  input: ShadowOuputdataset
    -> tmp: ShadowOutputdataset
  -> formats.out: OutputDataset

cps
 */

  def extract(formats: OutputFormats, input: ShadowOutputDataset, dictionary: Dictionary): RepositoryTIO[Unit] = RepositoryT.fromResultTIO(repository => {
    val tmpShadow: ShadowOutputDataset = ???
    formats.outputs.traverse {
      case (DenseFormat(format), output) =>
        println(s"Storing extracted data '$input' to '${output.location}'")
        GroupByEntityOutput.createWithDictionary(repository, input, tmpShadow, dictionary, format match {
          case DelimitedFile(delim) => GroupByEntityFormat.DenseText(delim, formats.missingValue)
          case ThriftFile           => GroupByEntityFormat.DenseThrift
        })
      case (SparseFormat(ThriftFile), output) =>
        println(s"Storing extracted data '$input' to '${output.location.path}'")
        GroupByEntityOutput.createWithDictionary(repository, input, tmpShadow, dictionary, GroupByEntityFormat.SparseThrift)
      case (SparseFormat(DelimitedFile(delim)), output) =>
        println(s"Storing extracted data '$input' to '${output.location.path}'")
//        SparseOutput.extractWithDictionary(repository, input, output, dictionary, delim, formats.missingValue)
        ???
    }.void
  })
}
