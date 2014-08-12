package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import scalaz.{Name => _, _}, Scalaz._

class DictionaryTextStorageV2Spec extends Specification with ScalaCheck { def is = s2"""
  Parsing a dictionary on:
    Sanity check                                $sanityCheck
    Invalid inputs will result in a failure     $invalidInput
    Parse any valid dictionary                  $anyDictionary
"""

  import DictionaryTextStorageV2._

  def sanityCheck = {
    val dict = Dictionary(Map(
      FeatureId(Name.unsafe("a"), "b") -> FeatureMeta(StringEncoding, Some(BinaryType), "", List("*")),
      FeatureId("c", "d") -> FeatureMeta(StructEncoding(Map("x" -> StructEncodedValue(BooleanEncoding))), None, "hello", Nil)
    ))
    fromString(
      """a:b|encoding=string|type=binary|tombstone=*
        |c:d|encoding=(x: boolean)|description=hello""".stripMargin) ==== dict.success
  }

  def invalidInput = seqToResult(List(
    "invalid",
    "encoding=string",
    "invalid|encoding=string",
    "a:b",
    "a:b|encoding=abc",
    "a:b|encoding=string|type=xxx"
  ).map(fromString).map(_.toEither must beLeft
    ))

  def anyDictionary = prop((dict: Dictionary) =>
    fromString(delimitedString(dict)) ==== dict.success
  )
}
