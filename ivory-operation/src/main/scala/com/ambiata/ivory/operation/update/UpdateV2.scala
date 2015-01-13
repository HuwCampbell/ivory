package com.ambiata.ivory.operation.update

import com.ambiata.ivory.storage.control._

object UpdateV2 {

  def update: RepositoryTIO[Unit] =
    // The V0 code has been fixed, and we just need to re-run
    UpdateV0.updateSnapshots
}
