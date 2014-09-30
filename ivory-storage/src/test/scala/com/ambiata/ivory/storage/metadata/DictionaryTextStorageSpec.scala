package com.ambiata.ivory.storage.metadata

import org.specs2._
import scalaz.{Name => _, _}, Scalaz._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._

import com.ambiata.ivory.core._

class DictionaryTextStorageSpec extends Specification { def is = s2"""

  Parsing a dictionary entry can:
    extract to completion when all fields are valid $e1
    catch invalid encodings                         $e2
    catch invalid types                             $e3
    parse all encoding                              $parseEncoding
    parse all types                                 $parseType

  Given a dictionary file we can:
    load it successfully if it is valid             $e4
    fail if it has invalid entries                  $e5
                                                    """

  def e1 = {
    val entry = "demo|postcode|string|categorical|Postcode|☠"
    DictionaryTextStorage.parseDictionaryEntry(entry) ==== (FeatureId(Name("demo"), "postcode"), ConcreteDefinition(StringEncoding, Some(CategoricalType), "Postcode", List("☠"))).success
  }

  def e2 = {
    val entry = "demo|postcode|strin|categorical|Postcode|☠"
    DictionaryTextStorage.parseDictionaryEntry(entry).toEither must beLeft(contain("not a valid encoding: 'strin'"))
  }

  def e3 = {
    val entry = "demo|postcode|string|cat|Postcode|☠"
    DictionaryTextStorage.parseDictionaryEntry(entry).toEither must beLeft(contain("not a valid feature type: 'cat'"))
  }

  def e4 = {
    val reference = Reference(PosixStore("ivory-storage" </> "src" </> "test" </> "resources")) </> "good_dictionary.txt"
    DictionaryTextStorage.fromFileIO(reference).run.unsafePerformIO().toDisjunction must_== Dictionary(List(
     Definition.concrete(FeatureId(Name("demo"), "gender"), StringEncoding, Some(CategoricalType), "Gender", List("☠")),
     Definition.concrete(FeatureId(Name("widgets"), "count.1W"), IntEncoding, Some(NumericalType), "Count in the last week", List("☠")),
     Definition.concrete(FeatureId(Name("demo"), "postcode"), StringEncoding, Some(CategoricalType), "Postcode", List("☠"))
   )).right
  }

  def e5 = {
    val reference = Reference(PosixStore("ivory-storage" </> "src" </> "test" </> "resources")) </> "bad_dictionary.txt"
    DictionaryTextStorage.fromFileIO(reference).run.unsafePerformIO().toEither must beLeft
  }

  def parseEncoding =
    seqToResult(List(BooleanEncoding, IntEncoding, LongEncoding, DoubleEncoding, StringEncoding).map {
      enc => DictionaryTextStorage.parseEncoding(Encoding.render(enc)) ==== enc.success
    })

  def parseType =
    seqToResult(List(NumericalType, ContinuousType, CategoricalType, BinaryType).map {
      ty => DictionaryTextStorage.parseType(Type.render(ty)) ==== ty.success
    })
}
