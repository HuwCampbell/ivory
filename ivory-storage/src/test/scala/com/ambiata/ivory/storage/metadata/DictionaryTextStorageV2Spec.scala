package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import scalaz.{Name => _, _}, Scalaz._

class DictionaryTextStorageV2Spec extends Specification with ScalaCheck { def is = s2"""
  Parsing a dictionary on:
    Invalid inputs will result in a failure     $invalidInput
    Parse any valid dictionary                  $anyDictionary
"""

  import DictionaryTextStorageV2._

  def invalidInput = seqToResult(List(
    "invalid",
    "encoding=string",
    "invalid|encoding=string",
    "a:b",
    "a:b|encoding=abc",
    "a:b|encoding=string|type=xxx",
    "a:b|alias=xy",
    "a:b|alias=x:y|encoding=string",
    "a:b|alias=x:y|window=foo",
    "a:b|alias=x:y|window=m days",
    "a:b|alias=x:y|window=4 dayz"
  ).map(fromString).map(_.toEither must beLeft))

  def anyDictionary = prop((dict: Dictionary) =>
    fromString(delimitedString(dict)) ==== dict.success
  )
}
