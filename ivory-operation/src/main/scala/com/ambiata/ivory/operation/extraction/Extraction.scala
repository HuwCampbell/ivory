package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.sync._
import com.ambiata.ivory.operation.extraction.output._
import com.ambiata.notion.core._

import java.util.UUID

import scalaz.Scalaz._

object Extraction {

/*
  input: ShadowOuputdataset
    -> tmp: ShadowOutputdataset
  -> formats.out: OutputDataset

cps
 */

  def extract(formats: OutputFormats, input: ShadowOutputDataset, dictionary: Dictionary, cluster: Cluster): RepositoryTIO[Unit] = RepositoryT.fromResultTIO(repository => for {
    t <- Repository.tmpDir(repository)
    tmpShadow = ShadowOutputDataset(HdfsLocation(t.name))
    _ <- (formats.outputs.traverse {
      case (DenseFormat(format), output) =>
        println(s"Storing extracted data '$input' to '${output.location}'")
        GroupByEntityOutput.createWithDictionary(repository, input, tmpShadow, dictionary, format match {
          case DelimitedFile(delim) => GroupByEntityFormat.DenseText(delim, formats.missingValue)
          case ThriftFile           => GroupByEntityFormat.DenseThrift
        }) >>
          SyncExtract.outputDataset(tmpShadow, cluster, output)
      case (SparseFormat(ThriftFile), output) =>
        println(s"Storing extracted data '$input' to '${output.location}'")
        GroupByEntityOutput.createWithDictionary(repository, input, tmpShadow, dictionary, GroupByEntityFormat.SparseThrift) >>
        SyncExtract.outputDataset(tmpShadow, cluster, output)

      case (SparseFormat(DelimitedFile(delim)), output) =>
        println(s"Storing extracted data '$input' to '${output.location}'")
        SparseOutput.extractWithDictionary(repository, input, tmpShadow, dictionary, delim, formats.missingValue) >>
        SyncExtract.outputDataset(tmpShadow, cluster, output)
    }.void)

  } yield ())
}
