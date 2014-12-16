package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._

import scalaz.effect.IO

object SparseOutput {
  def extractWithDictionary(repository: Repository, input: ShadowOutputDataset, output: ShadowOutputDataset, dictionary: Dictionary,
                            delim: Char, missing: String, escaped: Boolean): ResultTIO[Unit] = for {
    hdfsRepo <- repository.asHdfsRepository[IO]
    in        = input.hdfsPath
    out       = output.hdfsPath
    _        <- SparseOutputJob.run(hdfsRepo.configuration, dictionary, in, out, missing, delim, escaped, hdfsRepo.codec)
    } yield ()
}
