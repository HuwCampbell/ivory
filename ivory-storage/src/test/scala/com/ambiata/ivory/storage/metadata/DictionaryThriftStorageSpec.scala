package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultMatcher._
import org.scalacheck.Arbitrary, Arbitrary._
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._

class DictionaryThriftStorageSpec extends Specification with ScalaCheck {
  def is = s2"""

  Given a dictionary we can:
    store and then load it successfully             $e1
    fail on no dictionaries                         $empty
    load identifier dictionaries first              $identifierFirst
    load deprecated date based format               $dateLoad
    load and migrate date based format              $loadMigrate
    load from identifier                            $loadIdentifier
                                                    """

  def e1 = prop((dict: Dictionary) => run { (loader, _) =>
    loader.store(dict) >> loader.load
  } must beOkValue(dict))

  def empty = prop((dict: Dictionary) => run { (loader, _) =>
    loader.load
  }.isError)

  def identifierFirst = prop((dict: Dictionary) => run { (loader, dir) =>
    storeDateDicts(dict, dir) >> loader.store(dict) >> loader.load
  } must beOkValue(dict))

  def dateLoad = prop((dict: PrimitiveDictionary) => run { (loader, dir) =>
    storeDateDicts(dict.dict, dir) >> loader.load
  } must beOkValue(dict.dict))

  def loadMigrate = prop((dict: PrimitiveDictionary) => run { (loader, dir) =>
    storeDateDicts(dict.dict, dir) >> loader.loadMigrate
  } must beOkValue(Some(Identifier.initial -> dict.dict)))

  def loadIdentifier = prop((dict: Dictionary) => run { (loader, dir) =>
    loader.store(dict) >>= (id => loader.loadFromId(id._1))
  } must beOkValue(Some(dict)))

  private def storeDateDicts(dict: Dictionary, dir: FilePath): ResultTIO[Unit] = {
    import DictionaryTextStorage._
    def storeText(name: String) =
      Repository.fromLocalPath(dir).toStore.utf8.write(Repository.dictionaries </> name, delimitedString(dict))
    storeText("2004-03-12")
  }

  private def run[A](f: (DictionaryThriftStorage, FilePath) => ResultTIO[A]): Result[A] =
    Temporary.using(dir => f(DictionaryThriftStorage(Repository.fromLocalPath(dir)), dir)).run.unsafePerformIO()

  // Text dictionaries can only handle primitive encoding _with_ types and _at least_ one tombstone
  case class PrimitiveDictionary(dict: Dictionary)
  implicit def PrimitiveDictionaryArbitrary: Arbitrary[PrimitiveDictionary] =
    Arbitrary(arbitrary[Dictionary].map(d => d.copy(meta = d.meta.filter {
      case (k, Concrete(m)) => Encoding.isPrimitive(m.encoding) && m.ty.isDefined && m.tombstoneValue.nonEmpty && !m.desc.contains("\"")
      case (k, _: Virtual)  => false
    })).map(PrimitiveDictionary))
}
