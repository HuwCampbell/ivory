package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.data.Identifier
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
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
  }.map(_.byFeatureId) must beOkValue(dict.byFeatureId))

  def empty = prop((dict: Dictionary) => run { (loader, _) =>
    loader.load
  }.isError)

  def identifierFirst = prop((dict: Dictionary) => run { (loader, dir) =>
    storeDateDicts(dict, dir) >> loader.store(dict) >> loader.load
  }.map(_.byFeatureId) must beOkValue(dict.byFeatureId))

  def dateLoad = prop((dict: PrimitiveDictionary) => run { (loader, dir) =>
    storeDateDicts(dict.dict, dir) >> loader.load
  } must beOkValue(dict.dict))

  def loadMigrate = prop((dict: PrimitiveDictionary) => run { (loader, dir) =>
    storeDateDicts(dict.dict, dir) >> loader.loadMigrate
  } must beOkValue(Some(DictionaryId(Identifier.initial) -> dict.dict)))

  def loadIdentifier = prop((dict: Dictionary) => run { (loader, dir) =>
    loader.store(dict) >>= (id => loader.loadFromId(id))
  }.map(_.map(_.byFeatureId)) must beOkValue(Some(dict.byFeatureId)))

  private def storeDateDicts(dict: Dictionary, dir: DirPath): ResultTIO[Unit] = {
    import DictionaryTextStorage._
    PosixStore(dir).utf8.write(Repository.dictionaries / "2004-03-12", delimitedString(dict))
  }

  private def run[A](f: (DictionaryThriftStorage, DirPath) => ResultTIO[A]): Result[A] =
    Temporary.using(dir => f(DictionaryThriftStorage(LocalRepository(LocalLocation(dir))), dir)).run.unsafePerformIO()

  // Text dictionaries can only handle primitive encoding _with_ types and _at least_ one tombstone
  case class PrimitiveDictionary(dict: Dictionary)
  implicit def PrimitiveDictionaryArbitrary: Arbitrary[PrimitiveDictionary] =
    Arbitrary(arbitrary[Dictionary].map(d => d.copy(definitions = d.definitions.filter {
      case Concrete(k, m) =>
        Encoding.isPrimitive(m.encoding) && m.ty.isDefined && m.tombstoneValue.nonEmpty && !m.desc.contains("\"")
      case Virtual(k, _) =>
        false
    })).map(PrimitiveDictionary))
}
