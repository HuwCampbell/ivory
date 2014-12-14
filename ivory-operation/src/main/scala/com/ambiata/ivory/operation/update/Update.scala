package com.ambiata.ivory.operation.update

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._

/**
 * NOTE: There is no "Spec" for this, instead we rely on the data-regression cli test which uses actual old Ivory versions.
 */
object Update {

  def update: RepositoryTIO[Unit] = for {
    config <- Metadata.configuration
    _      <- config.metadata match {
      case MetadataVersion.Unknown(e) =>
        RepositoryT.fromRIO(_ => ResultT.failIO[Unit](s"Unknown version: $e"))
      case MetadataVersion.V0 =>
        UpdateV0.update >> incrementVersion(config, MetadataVersion.V1)
      case MetadataVersion.V1 =>
        ().pure[RepositoryTIO]
    }
  } yield ()

  def incrementVersion(config: RepositoryConfig, v: MetadataVersion): RepositoryTIO[Unit] = for {
    id <- RepositoryConfigTextStorage.store(config.copy(metadata = v))
    _  <- Metadata.incrementCommitRepositoryConfig(id)
  } yield ()
}
