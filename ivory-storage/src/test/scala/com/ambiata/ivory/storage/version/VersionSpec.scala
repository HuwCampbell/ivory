package com.ambiata.ivory.storage.version

import com.ambiata.mundane.store._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.specs2.Specification
import com.ambiata.ivory.core.TemporaryReferences._
import scalaz._, Scalaz._

class VersionSpec extends Specification { def is = s2"""

Version
-------

  Read empty                                     $empty
  Read and write                                 $readWrite

"""

  def empty = withRepository(Posix) { repository =>
    Version.read(repository, Key.Root)
  } must beOk.not


  def readWrite = withRepository(Posix) { repository =>
    Version.write(repository, Key("version"), Version("a")) >> Version.read(repository, Key("version"))
  } must beOkValue(Version("a"))
}
