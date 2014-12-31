package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._

object SparseOutput {
  def extractWithDictionary(repository: Repository, input: ShadowOutputDataset, output: ShadowOutputDataset, dictionary: Dictionary,
                            delim: Delimiter, missing: String, escaping: TextEscaping): RIO[Unit] = for {
    hdfsRepo <- repository.asHdfsRepository
    in        = input.hdfsPath
    out       = output.hdfsPath
    _        <- SparseOutputJob.run(hdfsRepo.configuration, dictionary, in, out, missing, delim, escaping, hdfsRepo.codec)
    } yield ()
}
