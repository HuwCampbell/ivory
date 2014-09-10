package com.ambiata.ivory.storage.version

import com.ambiata.ivory.core.Reference
import com.ambiata.mundane.io._

import scalaz.Scalaz._
import scalaz._

case class Version(override val toString: String)

object Version {

  private val VERSION = ".version".toFilePath

  def read[F[+_] : Functor](path: Reference[F]): F[Version] =
    (path </> VERSION).run(_.utf8.read).map(v => new Version(v.trim))

  def write[F[+_]](path: Reference[F], version: Version): F[Unit] =
    (path </> VERSION).run(s => s.utf8.write(_, version.toString))
}
