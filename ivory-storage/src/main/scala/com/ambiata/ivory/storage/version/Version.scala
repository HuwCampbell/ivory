package com.ambiata.ivory.storage.version

import com.ambiata.ivory.core.{ReferenceStore, Reference}
import com.ambiata.mundane.io._

import scalaz.Scalaz._
import scalaz._

case class Version(override val toString: String)

object Version {

  private val VERSION: FileName = ".version"

  def read[F[_] : Functor](path: Reference[F]): F[Version] =
    ReferenceStore.readUtf8[F](path </> VERSION).map(v => new Version(v.trim))

  def write[F[_]: Monad](path: Reference[F], version: Version): F[Unit] =
    ReferenceStore.writeUtf8[F](path </> VERSION, version.toString)
}
