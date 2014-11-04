package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.lookup.ReducerSize
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import scalaz.effect.IO

object PivotOutput {

  def createPivotWithDictionary(repository: Repository, input: IvoryLocation, output: IvoryLocation, dictionary: Dictionary, delim: Char, missing: String): ResultTIO[Unit] = for {
    hdfsRepo       <- repository.asHdfsRepository[IO]
    inputLocation  <- input.asHdfsIvoryLocation[IO]
    in             =  inputLocation.toHdfsPath
    outputLocation <- output.asHdfsIvoryLocation[IO]
    out            =  outputLocation.toHdfsPath
    reducers       <- ReducerSize.calculate(in, 256.mb).run(hdfsRepo.configuration)
  } yield PivotOutputJob.run(hdfsRepo.configuration, dictionary, in, out, missing, delim, reducers, hdfsRepo.codec)
}
