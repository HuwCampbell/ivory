package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.data.TemporaryStoreRun._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.Store
import com.ambiata.mundane.testing.ResultMatcher._
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
  val base = DirPath.Empty </> "base"
  def ref[F[_]](store: Store[F]) = Reference(store, base)
  val fileName: FileName = "fileName"

  def getEmpty = run { store =>
    IdentifierStorage.get(ref(store))
  } must beOkValue(Scalaz.none)

  def getEmptyFail = run { store =>
    IdentifierStorage.getOrFail(ref(store))
  }.toOption ==== Scalaz.none

  def writeAndGet = run { store =>
    IdentifierStorage.write(ref(store), fileName, ByteVector.empty) >> IdentifierStorage.getOrFail(ref(store))
  } must beOkValue(Identifier.initial)

  def writeMultiple = run { store =>
    for {
      _  <- IdentifierStorage.write(ref(store) </> "a", fileName, ByteVector.empty)
      i1 <- IdentifierStorage.getOrFail(ref(store) </> "a")
      _  <- IdentifierStorage.write(ref(store) </> "a", fileName, ByteVector.empty)
      i2 <- IdentifierStorage.getOrFail(ref(store) </> "a")
      _  <- IdentifierStorage.write(ref(store) </> "b", fileName, ByteVector.empty)
      i3 <- IdentifierStorage.getOrFail(ref(store) </> "a")
    } yield (i1, i2, i3)
  } must beOkValue((Identifier("00000000"), Identifier("00000001"), Identifier("00000001")))


}
