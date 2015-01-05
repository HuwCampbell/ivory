package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core.Identifier
import com.ambiata.ivory.core.RepositoryTemporary._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType._
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._
import scodec.bits.ByteVector

import scalaz.Scalaz._
import scalaz.{Store=>_,_}

class IdentifierStorageSpec extends Specification with ScalaCheck { def is = s2"""

Identifier Storage Spec
-----------------------

  Get empty                                  $getEmpty
  Get empty fail                             $getEmptyFail
  Write and then get                         $writeAndGet
  Write multiple times and then get          $writeMultiple
  List Ids                                   $listIds          ${tag("store")}
  Next or fail                               $nextIdOrFailOk   ${tag("store")}
  Next or fail when it fails                 $nextIdOrFailFail ${tag("store")}
  Next or fail first                         $nextIdOrFailFirst ${tag("store")}

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

  def listIds = prop { (n: Byte) =>
    val root = Key.Root / "subdir"
    val max = Math.abs(n) % 10
    withRepository(Posix) { repository =>
      (0 until max).toList.traverseU(_ => IdentifierStorage.write(repository, root, keyName, ByteVector.empty)) >>
        IdentifierStorage.listIds(repository, root)
    } must beOkValue(
      (0 until max).foldLeft(List(Identifier.initial))((i, _) => i.headOption.flatMap(_.next).toList ++ i).tail.reverse
    )
  }

  def nextIdOrFailOk = prop { (identifier: Identifier) =>
    withRepository(Posix) { repository =>
      repository.store.utf8.write(Key.Root / identifier.asKeyName, "") >>
        IdentifierStorage.nextIdOrFail(repository, Key.Root)
    } must beOkValue(identifier.next.get)
  }

  def nextIdOrFailFail = {
    withRepository(Posix) { repository =>
      repository.store.utf8.write(Key.Root / Identifier.unsafe(0xffffffff).asKeyName, "") >>
        IdentifierStorage.nextIdOrFail(repository, Key.Root)
    } must beFail
  }

  def nextIdOrFailFirst =
    withRepository(Posix) { repository =>
      IdentifierStorage.nextIdOrFail(repository, Key.Root)
    } must beOkValue(Identifier.initial)
}
