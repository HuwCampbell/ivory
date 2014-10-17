package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.operation.extraction.Snapshot
import com.ambiata.ivory.operation.extraction.squash.{SquashConfig, SquashJob}
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata.SnapshotManifest
import com.ambiata.mundane.control._

/**
 * Takes a snapshot and stores as EAV text
 */
object EavOutput {

  /**
   * Take a snapshot first then extract EAV text
   */
  def extractFromSnapshot(repository: Repository, output: IvoryLocation, delim: Char, tombstone: String,
                          meta: SnapshotManifest, conf: SquashConfig): ResultTIO[Unit] = for {
    dict <- Snapshot.dictionaryForSnapshot(repository, meta)
    _    <- SquashJob.squashFromSnapshotWith(repository, dict, meta, output, conf)(key =>
              extractWithDictionary(repository, repository.toIvoryLocation(key), output, dict, delim, tombstone))
  } yield ()

  /**
   * Extract EAV text from a given snapshot (input) to output
   */
  def extractFromChord(repository: Repository, input: IvoryLocation, output: IvoryLocation, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    dictionary  <- latestDictionaryFromIvory(repository)
    _            = NotImplemented.chordSquash()
    _           <- extractWithDictionary(repository, input, output, dictionary.removeVirtualFeatures, delim, tombstone)
  } yield ()

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
