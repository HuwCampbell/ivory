package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core.TemporaryRepositories
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import com.ambiata.notion.core.TemporaryType

object TemporaryRepositoriesT {

  def withRepositoryT[A](temporaryType: TemporaryType)(f: RepositoryTIO[A]): ResultTIO[A] = {
    TemporaryRepositories.withRepository(temporaryType)(repo => f.toIvoryT(repo).run(IvoryRead.create))
  }
}
