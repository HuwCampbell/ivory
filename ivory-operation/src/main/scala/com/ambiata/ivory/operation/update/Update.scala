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
        RepositoryT.fromRIO(_ => RIO.failIO[Unit](s"Unknown version: $e"))
      case MetadataVersion.V0 =>
        // Skip to V3 here - both V2 and V3 are included
        UpdateV0.update >> incrementVersion(config, MetadataVersion.V4)
      case MetadataVersion.V1 =>
        UpdateV1.update >> UpdateV2.update >> incrementVersion(config, MetadataVersion.V4)
      case MetadataVersion.V2 =>
        UpdateV2.update >> incrementVersion(config, MetadataVersion.V4)
      case MetadataVersion.V3 =>
        incrementVersion(config, MetadataVersion.V4)
      case MetadataVersion.V4 =>
        RepositoryT.fromRIO(_ => RIO.putStrLn("Repository is already at latest version [v4]"))
    }
  } yield ()

  def incrementVersion(config: RepositoryConfig, v: MetadataVersion): RepositoryTIO[Unit] = for {
    id <- RepositoryConfigTextStorage.store(config.copy(metadata = v))
    _  <- Metadata.incrementCommitRepositoryConfig(id)
  } yield ()
}
