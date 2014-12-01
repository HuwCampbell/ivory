package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.lookup.ReducerSize
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._

import org.apache.hadoop.fs.Path

import scalaz.effect.IO

object GroupByEntityOutput {
  def createWithDictionary(repository: Repository, input: ShadowOutputDataset, output: ShadowOutputDataset, dictionary: Dictionary,
                           format: GroupByEntityFormat): ResultTIO[Unit] = for {
    hdfsRepo       <- repository.asHdfsRepository[IO]
    in             =  new Path(input.location.path)
    out            =  new Path(output.location.path)
    reducers       <- ReducerSize.calculate(in, 256.mb).run(hdfsRepo.configuration)
    _              <- GroupByEntityOutputJob.run(hdfsRepo.configuration, dictionary, in, out, format, reducers, hdfsRepo.codec)
  } yield ()
}

sealed trait GroupByEntityFormat

object GroupByEntityFormat {
  case class DenseText(delim: Char, missing: String, escaped: Boolean) extends GroupByEntityFormat
  case object DenseThrift extends GroupByEntityFormat
  case object SparseThrift extends GroupByEntityFormat
}
