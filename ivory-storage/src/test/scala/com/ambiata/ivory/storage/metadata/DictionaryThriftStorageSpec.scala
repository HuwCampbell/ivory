package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.notion.core._
import com.ambiata.mundane.testing.ResultMatcher._
import org.scalacheck.Arbitrary, Arbitrary._
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._

class DictionaryThriftStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Given a dictionary we can:
    store and then load it successfully             $e1
    fail on no dictionaries                         $empty
    load identifier dictionaries first              $identifierFirst
    load deprecated date based format               $dateLoad
    load and migrate date based format              $loadMigrate
    load from identifier                            $loadIdentifier
                                                    """

  def e1 = prop((local: LocalTemporary, dict: Dictionary) => run(local) { (loader, _) =>
    loader.store(dict) >> loader.load
  }.map(_.byFeatureId) must beOkValue(dict.byFeatureId))

  def empty = prop((local: LocalTemporary, dict: Dictionary) => run(local) { (loader, _) =>
    loader.load
  }.isError)

  def identifierFirst = prop((local: LocalTemporary, dict: Dictionary) => run(local) { (loader, dir) =>
    storeDateDicts(dict, dir) >> loader.store(dict) >> loader.load
  }.map(_.byFeatureId) must beOkValue(dict.byFeatureId))

  def dateLoad = prop((local: LocalTemporary, dict: PrimitiveDictionary) => run(local) { (loader, dir) =>
    storeDateDicts(dict.dict, dir) >> loader.load
  } must beOkValue(setState(dict.dict)))

  def loadMigrate = prop((local: LocalTemporary, dict: PrimitiveDictionary) => run(local) { (loader, dir) =>
    storeDateDicts(dict.dict, dir) >> loader.loadMigrate
  } must beOkValue(Some(DictionaryId(Identifier.initial) -> setState(dict.dict))))

  def loadIdentifier = prop((local: LocalTemporary, dict: Dictionary) => run(local) { (loader, dir) =>
    loader.store(dict) >>= (id => loader.loadFromId(id))
  }.map(_.map(_.byFeatureId)) must beOkValue(Some(dict.byFeatureId)))

  private def storeDateDicts(dict: Dictionary, dir: DirPath): RIO[Unit] = {
    import DictionaryTextStorage._
    PosixStore(dir).utf8.write(Repository.dictionaries / "2004-03-12", delimitedString(dict))
  }

  private def run[A](local: LocalTemporary)(f: (DictionaryThriftStorage, DirPath) => RIO[A]): Result[A] = (for {
    d <- local.directory
    r <- f(DictionaryThriftStorage(LocalRepository.create(d)), d)
  } yield r).unsafePerformIO

  // Text dictionaries can only handle primitive encoding _with_ types and _at least_ one tombstone
  case class PrimitiveDictionary(dict: Dictionary)
  implicit def PrimitiveDictionaryArbitrary: Arbitrary[PrimitiveDictionary] =
    Arbitrary(arbitrary[Dictionary].map(d => d.copy(definitions = d.definitions.filter {
      case Concrete(k, m) =>
        Encoding.isPrimitive(m.encoding) && m.ty.isDefined && m.tombstoneValue.nonEmpty && !m.desc.contains("\"")
      case Virtual(k, _) =>
        false
    })).map(PrimitiveDictionary))

  /* Older, date based dictionaries do not support sets, need to force all modes to state for testing legacy migration . */
  def setState(d: Dictionary): Dictionary = Dictionary(d.definitions.map({
    case Concrete(id, definition) =>
      Concrete(id, definition.copy(mode = Mode.State))
    case Virtual(id, definition) =>
      Virtual(id, definition)
  }))

}
