package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.mundane.control._

/**
 * Takes a snapshot and stores as EAV text
 */
object EavOutput {

  /**
   *  HdfsRepository
   */
  def extractWithDictionary(repository: Repository, input: IvoryLocation, output: IvoryLocation, dictionary: Dictionary, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    hdfsRepo        <- downcast[Repository, HdfsRepository](repository, s"Eav extract only works with Hdfs repositories currently, got '$repository'")
    inputLocation   <- downcast[IvoryLocation, HdfsIvoryLocation](input, s"Eav extract can only read from HDFS currently, got '${input.show}'")
    in              =  inputLocation.toHdfsPath
    outputLocation  <- downcast[IvoryLocation, HdfsIvoryLocation](output, s"Eav extract can only read from HDFS currently, got '${output.show}'")
    out             =  outputLocation.toHdfsPath
    } yield EavOutputJob.run(hdfsRepo.configuration, dictionary, in, out, tombstone, delim, hdfsRepo.codec)
}
