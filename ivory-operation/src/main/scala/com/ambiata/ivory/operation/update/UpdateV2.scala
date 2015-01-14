package com.ambiata.ivory.operation.update

import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.manifest.FactsetManifest
import com.ambiata.ivory.storage.metadata.Metadata
import scalaz._, Scalaz._

object UpdateV2 {

  def update: RepositoryTIO[Unit] =
    for {
      // The V0 code has been fixed, and we just need to re-run
      _ <- UpdateV0.updateSnapshots
      // Even empty factsets should have a manifest
      _ <- updateEmptyFactsts
    } yield ()

  def updateEmptyFactsts: RepositoryTIO[Unit] = RepositoryT.fromRIO(repository =>
    Metadata.findFactsets(repository).flatMap(_.filterM {
      f => FactsetManifest.io(repository, f).exists.map(!_)
    }).flatMap(UpdateV0.updateFactsetsIds(repository, _))
  )
}
