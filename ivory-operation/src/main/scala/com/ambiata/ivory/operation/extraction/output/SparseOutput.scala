package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import scalaz.effect.IO

object SparseOutput {
  def extractWithDictionary(repository: Repository, input: IvoryLocation, output: IvoryLocation, dictionary: Dictionary, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    hdfsRepo        <- repository.asHdfsRepository[IO]
    inputLocation   <- input.asHdfsIvoryLocation[IO]
    in              =  inputLocation.toHdfsPath
    outputLocation  <- output.asHdfsIvoryLocation[IO]
    out             =  outputLocation.toHdfsPath
    } yield SparseOutputJob.run(hdfsRepo.configuration, dictionary, in, out, tombstone, delim, hdfsRepo.codec)
}
