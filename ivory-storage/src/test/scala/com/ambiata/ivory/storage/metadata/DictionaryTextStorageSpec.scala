package com.ambiata.ivory.storage.metadata

import org.specs2._
import scalaz._, Scalaz._
import com.ambiata.mundane.io._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class DictionaryTextStorageSpec extends Specification with ScalaCheck { def is = s2"""

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
    DictionaryTextStorage.parseDictionaryEntry(entry) ==== (FeatureId(Namespace("demo"), "postcode"),
      ConcreteDefinition(StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "Postcode", List("☠"))).success
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
    val resource = FilePath.unsafe(getClass.getClassLoader.getResource("good_dictionary.txt").getFile)
    val location = IvoryLocation.fromFilePath(resource)
    DictionaryTextStorage.fromSingleFile(location).unsafePerformIO.toDisjunction must_== Dictionary(List(
     Definition.concrete(FeatureId(Namespace("demo"), "gender"), StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "Gender", List("☠")),
     Definition.concrete(FeatureId(Namespace("widgets"), "count.1W"), IntEncoding.toEncoding, Mode.State, Some(NumericalType), "Count in the last week", List("☠")),
     Definition.concrete(FeatureId(Namespace("demo"), "postcode"), StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "Postcode", List("☠"))
   )).right
  }

  def e5 = {
    val location = IvoryLocation.fromFilePath("ivory-storage" </> "src" </> "test" </> "resources" <|> "bad_dictionary.txt")
    DictionaryTextStorage.fromSingleFile(location).unsafePerformIO.toEither must beLeft
  }

  def parseEncoding = prop { enc: PrimitiveEncoding =>
    DictionaryTextStorage.parseEncoding(Encoding.renderPrimitive(enc)) ==== enc.success
  }

  def parseType =
    seqToResult(List(NumericalType, ContinuousType, CategoricalType, BinaryType).map {
      ty => DictionaryTextStorage.parseType(Type.render(ty)) ==== ty.success
    })
}
