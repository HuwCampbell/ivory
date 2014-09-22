package com.ambiata.ivory.storage.version

import com.ambiata.ivory.core._
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.store._

case class Version(override val toString: String)

object Version {

  private val VERSION = KeyName.unsafe(".version")

  def read(repository: Repository, key: Key): ResultTIO[Version] =
    repository.store.utf8.read(key / VERSION).map(v => new Version(v.trim))

  def write(repository: Repository, key: Key, version: Version): ResultTIO[Unit] =
    repository.store.utf8.write(key / VERSION, version.toString)
}
