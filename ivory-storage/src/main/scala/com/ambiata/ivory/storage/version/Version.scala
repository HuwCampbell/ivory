package com.ambiata.ivory.storage.version

import com.ambiata.ivory.core._
import com.ambiata.mundane.control.RIO
import com.ambiata.notion.core._

// FIX rename or remove.
case class Version(override val toString: String)

object Version {

  private val VERSION = KeyName.unsafe(".version")

  def read(repository: Repository, key: Key): RIO[Version] =
    repository.store.utf8.read(key / VERSION).map(v => new Version(v.trim))

  def write(repository: Repository, key: Key, version: Version): RIO[Unit] =
    repository.store.utf8.write(key / VERSION, version.toString)
}
