package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.legacy.SnapshotMeta
import com.ambiata.ivory.operation.extraction.Snapshot
import com.ambiata.ivory.operation.extraction.squash.SquashJob
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs._

/**
 * Takes a snapshot and stores as EAV text
 */
object EavOutput {

  /**
   * Take a snapshot first then extract EAV text
   */
  def extractFromSnapshot(repository: Repository, output: ReferenceIO, delim: Char, tombstone: String, meta: SnapshotMeta): ResultTIO[Unit] = for {
    dict <- Snapshot.dictionaryForSnapshot(repository, meta)
    _    <- SquashJob.squashFromSnapshotWith(repository, dict, meta)(path =>
      extractWithDictionary(repository, repository.toReference(path), output, dict, delim, tombstone))
  } yield ()

  /**
   * Extract EAV text from a given snapshot (input) to output
   */
  def extractFromChord(repository: Repository, input: ReferenceIO, output: ReferenceIO, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    dictionary  <- latestDictionaryFromIvory(repository)
    _            = NotImplemented.chordSquash()
    _           <- extractWithDictionary(repository, input, output, dictionary.removeVirtualFeatures, delim, tombstone)
  } yield ()

  /**
   *  HdfsRepository
   */
  def extractWithDictionary(repository: Repository, input: ReferenceIO, output: ReferenceIO, dictionary: Dictionary, delim: Char, tombstone: String): ResultTIO[Unit] = for {
    hdfsRepo    <- downcast[Repository, HdfsRepository](repository, s"Eav extract only works with Hdfs repositories currently, got '$repository'")
    inputStore  <- downcast[Any, HdfsStore](input.store, s"Eav extract can only read from HDFS currently, got '$input'")
    in          =  (inputStore.base </> input.path).toHdfs
    outputStore <- downcast[Any, HdfsStore](output.store, s"Eav extract can only read from HDFS currently, got '$output'")
    out         =  (outputStore.base </> output.path).toHdfs
    } yield EavOutputJob.run(hdfsRepo.configuration, dictionary, in, out, tombstone, delim, hdfsRepo.codec)
}
