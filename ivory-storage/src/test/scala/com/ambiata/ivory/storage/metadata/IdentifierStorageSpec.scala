package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.core.TemporaryReferences._
import com.ambiata.mundane.store._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.specs2._
import scodec.bits.ByteVector

import scalaz.Scalaz._
import scalaz.{Store=>_,_}

class IdentifierStorageSpec extends Specification { def is = s2"""

Identifier Storage Spec
-----------------------

  Get empty                                  $getEmpty
  Get empty fail                             $getEmptyFail
  Write and then get                         $writeAndGet
  Write multiple times and then get          $writeMultiple

"""

  val keyName = KeyName.unsafe("keyname")

  def getEmpty = withRepository(Posix) { repository =>
    IdentifierStorage.get(repository, Key.Root)
  } must beOkValue(Scalaz.none)

  def getEmptyFail = withRepository(Posix) { repository =>
    IdentifierStorage.getOrFail(repository, Key.Root)
  } must beOk.not

  def writeAndGet = withRepository(Posix) { repository =>
    IdentifierStorage.write(repository, Key.Root, keyName, ByteVector.empty) >> IdentifierStorage.getOrFail(repository, Key.Root)
  } must beOkValue(Identifier.initial)

  def writeMultiple = withRepository(Posix) { repository =>
    for {
      _  <- IdentifierStorage.write(repository, Key("a"), keyName, ByteVector.empty)
      i1 <- IdentifierStorage.getOrFail(repository, Key("a"))
      _  <- IdentifierStorage.write(repository, Key("a"), keyName, ByteVector.empty)
      i2 <- IdentifierStorage.getOrFail(repository, Key("a"))
      _  <- IdentifierStorage.write(repository, Key("b"), keyName, ByteVector.empty)
      i3 <- IdentifierStorage.getOrFail(repository, Key("a"))
    } yield (i1, i2, i3)
  } must beOkValue((Identifier("00000000"), Identifier("00000001"), Identifier("00000001")))


}
