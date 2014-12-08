package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.sync._
import com.ambiata.ivory.operation.extraction.output._
import com.ambiata.mundane.control._

import java.util.UUID

import scalaz._, Scalaz._, effect.IO

object Extraction {

  def extract(formats: OutputFormats, input: ShadowOutputDataset, dictionary: Dictionary, cluster: Cluster): RepositoryTIO[Unit] = RepositoryT.fromResultTIO(repository => for {
    t <- Repository.tmpDir(repository)
    i = repository.toIvoryLocation(t)
    h <- i.asHdfsIvoryLocation[IO]
    s = ShadowOutputDataset(h.location)
    _ <- (formats.outputs.traverseU({ case (format, output) =>
      ResultT.io(println(s"Storing extracted data '$input' to '${output.location}'")) >> (format match {
        case DenseFormat(format) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, format match {
            case DelimitedFile(delim) => GroupByEntityFormat.DenseText(delim, formats.missingValue, false)
            case EscapedFile(delim)   => GroupByEntityFormat.DenseText(delim, formats.missingValue, true)
            case ThriftFile           => GroupByEntityFormat.DenseThrift
          })
        case SparseFormat(ThriftFile) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, GroupByEntityFormat.SparseThrift)
        case SparseFormat(DelimitedFile(delim)) =>
          SparseOutput.extractWithDictionary(repository, input, s, dictionary, delim, formats.missingValue, false)
        case SparseFormat(EscapedFile(delim)) =>
          SparseOutput.extractWithDictionary(repository, input, s, dictionary, delim, formats.missingValue, true)
      }) >> SyncExtract.outputDataset(s, cluster, output) }).void)
  } yield ())
}
