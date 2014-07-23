package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultMatcher._
import org.specs2.Specification
import scalaz._, Scalaz._

class DictionaryThriftStorageSpec extends Specification { def is = s2"""

  Given a dictionary we can:
    store and then load it successfully             $e1
    fail on no dictionaries                         $empty
    load identifier dictionaries first              $identifierFirst
    load deprecated date based format               $dateLoad
    load and migrate date based format              $loadMigrate
    load from identifier                            $loadIdentifier
                                                    """

  val dict = Arbitraries.TestDictionary

  def e1 = run { (loader, _) =>
    loader.store(dict) >> loader.load
  } must beOkValue(dict)

  def empty = run { (loader, _) =>
    loader.load
  }.isError

  def identifierFirst = run { (loader, dir) =>
    storeDateDicts(dir) >> loader.store(dict) >> loader.load
  } must beOkValue(dict)

  def dateLoad = run { (loader, dir) =>
    storeDateDicts(dir) >> loader.load
  } must beOkValue(dict.forNamespace("vegetables"))

  def loadMigrate = run { (loader, dir) =>
    storeDateDicts(dir) >> loader.loadMigrate
  } must beOkValue(Some(Identifier.initial -> dict.forNamespace("vegetables")))

  def loadIdentifier = run { (loader, dir) =>
    loader.store(dict) >>= (id => loader.loadFromId(id._1))
  } must beOkValue(Some(dict))

  private def storeDateDicts(dir: FilePath): ResultTIO[Unit] = {
    import DictionaryTextStorage._
    def storeText(name: String, ns: String) =
      Repository.fromLocalPath(dir).toStore.utf8.write(Repository.dictionaries </> name, delimitedString(dict.forNamespace(ns)))
    storeText("2004-03-12", "fruit") >> storeText("2006-08-34", "vegetables")
  }

  private def run[A](f: (DictionaryThriftStorage, FilePath) => ResultTIO[A]): Result[A] =
    Temporary.using(dir => f(DictionaryThriftStorage(Repository.fromLocalPath(dir)), dir)).run.unsafePerformIO()
}
