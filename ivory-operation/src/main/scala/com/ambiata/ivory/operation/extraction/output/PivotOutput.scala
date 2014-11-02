package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.lookup.ReducerSize
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._

/**
 * Takes a snapshot containing EAVTs
 *
 * and create a "dense" file where there is one line per entity id and all the values for that entity
 */
object PivotOutput {

  /**
   * Create a pivot on a HdfsRepository
   */
  def createPivotWithDictionary(repository: Repository, input: IvoryLocation, output: IvoryLocation, dictionary: Dictionary, delim: Char, missing: String): ResultTIO[Unit] = for {
    hdfsRepo       <- downcast[Repository, HdfsRepository](repository, s"Pivot only works with Hdfs repositories currently, got '$repository'")
    inputLocation  <- downcast[IvoryLocation, HdfsIvoryLocation](input, s"Pivot can only read from HDFS currently, got '${input.show}'")
    in             =  inputLocation.toHdfsPath
    outputLocation <- downcast[IvoryLocation, HdfsIvoryLocation](output, s"Pivot can only read from HDFS currently, got '${output.show}'")
    out            =  outputLocation.toHdfsPath
    reducers       <- ReducerSize.calculate(in, 256.mb).run(hdfsRepo.configuration)
  } yield PivotOutputJob.run(hdfsRepo.configuration, dictionary, in, out, missing, delim, reducers, hdfsRepo.codec)
}
