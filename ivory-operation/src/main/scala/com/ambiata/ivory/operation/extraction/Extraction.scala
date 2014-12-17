package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.sync._
import com.ambiata.ivory.operation.extraction.output._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._, effect.IO

object Extraction {

  def extract(formats: OutputFormats, input: ShadowOutputDataset, dictionary: Dictionary, cluster: Cluster): RepositoryTIO[Unit] = RepositoryT.fromResultTIO(repository => for {
    t <- Repository.tmpDir(repository)
    i = repository.toIvoryLocation(t)
    h <- i.asHdfsIvoryLocation[IO]
    s = ShadowOutputDataset(h.location)
    _ <- formats.outputs.traverseU({ case (format, output) =>
      ResultT.io(println(s"Storing extracted data '$input' to '${output.location}'")) >> (format match {
        case OutputFormat(Form.Sparse, FileFormat.Thrift) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, GroupByEntityFormat.SparseThrift)
        case OutputFormat(Form.Dense, FileFormat.Thrift) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, GroupByEntityFormat.DenseThrift)
        case OutputFormat(Form.Sparse, FileFormat.Text(delimiter, escaping)) =>
          SparseOutput.extractWithDictionary(repository, input, s, dictionary, delimiter, formats.missingValue, escaping)
        case OutputFormat(Form.Dense, FileFormat.Text(delimiter, escaping)) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, GroupByEntityFormat.DenseText(delimiter, formats.missingValue, escaping))
      }) >> SyncExtract.outputDataset(s, cluster, output) }).void
  } yield ())
}
