package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.sync._
import com.ambiata.ivory.operation.extraction.output._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._

import scalaz._, Scalaz._, effect.IO

object Extraction {

  def extract(formats: OutputFormats, input: ShadowOutputDataset, dictionary: Dictionary, cluster: Cluster): RepositoryTIO[Unit] = RepositoryT.fromRIO(repository =>
    formats.outputs.traverseU { case (format, output) => for {
      t <- Repository.tmpDir("extraction")
      i = repository.toIvoryLocation(t)
      h <- i.asHdfsIvoryLocation
      s = ShadowOutputDataset(h.location)
      _ <- RIO.io(println(s"Storing extracted data '$input' to '${output.location}'"))
      _ <- format match {
        case OutputFormat(Form.Sparse, FileFormat.Thrift) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, GroupByEntityFormat.SparseThrift)
        case OutputFormat(Form.Dense, FileFormat.Thrift) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, GroupByEntityFormat.DenseThrift)
        case OutputFormat(Form.Sparse, FileFormat.Text(delimiter, escaping)) =>
          SparseOutput.extractWithDictionary(repository, input, s, dictionary, delimiter, formats.missingValue, escaping)
        case OutputFormat(Form.Dense, FileFormat.Text(delimiter, escaping)) =>
          GroupByEntityOutput.createWithDictionary(repository, input, s, dictionary, GroupByEntityFormat.DenseText(delimiter, formats.missingValue, escaping))
      }
      _ <- metadata(input, s, cluster.io)
      _ <- SyncExtract.outputDataset(s, cluster, output)
    } yield () }.void
  )

  def metadata(input: ShadowOutputDataset, output: ShadowOutputDataset, lio: LocationIO): RIO[Unit] =
    List(".profile", ".manifest.json").traverseU(n => for {
      e <- lio.exists(input.location </> FileName.unsafe(n))
      // TODO Need to add a readUTF8 on LocationIO
      _ <- RIO.when(e, lio.readLines(input.location </> FileName.unsafe(n)) >>= (c => lio.writeUtf8Lines(output.location </> FileName.unsafe(n), c)))
    } yield ()).void
}
