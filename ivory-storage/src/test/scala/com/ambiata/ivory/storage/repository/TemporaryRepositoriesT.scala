package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import com.ambiata.notion.core.TemporaryType

import scalaz._, Scalaz._

object TemporaryRepositoriesT {
  def withRepositoryT[A](temporaryType: TemporaryType)(f: RepositoryTIO[A]): RIO[A] =
    RepositoryTemporary(temporaryType, s"temporary-${java.util.UUID.randomUUID().toString}").repository >>= (r =>
      f.toIvoryT(r).run(IvoryRead.create))
}
