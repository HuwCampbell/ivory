package com.ambiata.ivory.storage.legacy

import org.joda.time.LocalDate
import org.joda.time.LocalDateTime
import org.specs2._
import scalaz.{Name => _, _}, Scalaz._

import com.ambiata.ivory.core._

class DelimitedFactTextStorageSpec extends Specification { def is = s2"""

  Parsing a fact entry can:
    succeed when all fields are valid                     $e1
    fail if the encoding is incorrect                     $e2
    fail if the feature does not exist in the dictionary  $e3
                                                          """

  def e1 = {
    val entry = "928340|widgets:inbound.count.1W|35|2014-01-08 12:00:00"
    val dict = Dictionary(List(Definition.concrete(FeatureId(Name("widgets"), "inbound.count.1W"), IntEncoding, Some(NumericalType), "whatever", Nil)))
    DelimitedFactTextStorage.parseFact(dict, entry) must_==
      IntFact("928340", FeatureId(Name("widgets"), "inbound.count.1W"), Date(2014, 1, 8), Time(43200), 35).right
  }

  def e2 = {
    val entry = "928340|widgets:inbound.count.1W|thirty-five|2014-01-08 12:00:00"
    val dict = Dictionary(List(Definition.concrete(FeatureId(Name("widgets")  , "inbound.count.1W"), IntEncoding, Some(NumericalType), "whatever", Nil)))
    DelimitedFactTextStorage.parseFact(dict, entry).toEither must beLeft
  }

  def e3 = {
    val entry = "928340|widgets:inbound.count.1W|thirty-five|2014-01-08 12:00:00"
    val dict = Dictionary.empty
    DelimitedFactTextStorage.parseFact(dict, entry).toEither must beLeft
  }
}
