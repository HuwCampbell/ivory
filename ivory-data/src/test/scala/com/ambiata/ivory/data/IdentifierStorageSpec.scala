package com.ambiata.ivory.data

import com.ambiata.mundane.io._
import com.ambiata.mundane.store.PosixStore
import com.ambiata.mundane.testing.ResultMatcher._
import org.specs2._
import scalaz._, Scalaz._
import scodec.bits.ByteVector
import StoreTestUtil._

class IdentifierStorageSpec extends Specification { def is = s2"""

Identifier Storage Spec
-----------------------

  Get empty                                  $getEmpty
  Get empty fail                             $getEmptyFail
  Write and then get                         $writeAndGet
  Write multiple times and then get          $writeMultiple

"""

  def getEmpty = run { store =>
    IdentifierStorage.get(store, FilePath("a"))
  } must beOkValue(Scalaz.none)

  def getEmptyFail = run { store =>
    IdentifierStorage.getOrFail(store, FilePath("a"))
  }.toOption ==== Scalaz.none

  def writeAndGet = run { store =>
    val args = store -> FilePath("a")
    IdentifierStorage.write(FilePath.root, ByteVector.empty).tupled(args) >> IdentifierStorage.getOrFail.tupled(args)
  } must beOkValue(Identifier.initial -> FilePath("a/00000000"))

  def writeMultiple = run { store =>
    val args = store -> FilePath("a")
    for {
      _ <- IdentifierStorage.write(FilePath.root, ByteVector.empty).tupled(args)
      i1 <- IdentifierStorage.getOrFail.tupled(args)
      _ <- IdentifierStorage.write(FilePath.root </> "a", ByteVector.empty).tupled(args)
      i2 <- IdentifierStorage.getOrFail.tupled(args)
      _ <- IdentifierStorage.write(FilePath.root </> "b", ByteVector.empty).tupled(args)
      i3 <- IdentifierStorage.getOrFail.tupled(args)
    } yield (i1._2, i2._2, i3._2)
  } must beOkValue((FilePath("a/00000000"), FilePath("a/00000001"), FilePath("a/00000002")))
}
