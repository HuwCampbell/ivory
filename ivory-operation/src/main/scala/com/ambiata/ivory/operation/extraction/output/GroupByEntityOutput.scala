package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.lookup.ReducerSize
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import scalaz.effect.IO

object GroupByEntityOutput {
  def createWithDictionary(repository: Repository, input: IvoryLocation, output: IvoryLocation, dictionary: Dictionary,
                           format: GroupByEntityFormat): ResultTIO[Unit] = for {
    hdfsRepo       <- repository.asHdfsRepository[IO]
    inputLocation  <- input.asHdfsIvoryLocation[IO]
    in             =  inputLocation.toHdfsPath
    outputLocation <- output.asHdfsIvoryLocation[IO]
    out            =  outputLocation.toHdfsPath
    reducers       <- ReducerSize.calculate(in, 256.mb).run(hdfsRepo.configuration)
  } yield GropuByEntityOutputJob.run(hdfsRepo.configuration, dictionary, in, out, format, reducers, hdfsRepo.codec)
}

sealed trait GroupByEntityFormat

object GroupByEntityFormat {
  case class DenseText(delim: Char, missing: String) extends GroupByEntityFormat
  case object DenseThrift extends GroupByEntityFormat
  case object SparseThrift extends GroupByEntityFormat
}
