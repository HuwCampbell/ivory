package com.ambiata.ivory.extract

import scalaz._, Scalaz._
import com.ambiata.ivory.core._

object Validate {
  def validateFact(fact: Fact, dict: Dictionary): Validation[String, Fact] =
    dict.meta.get(fact.featureId).map(fm => validateEncoding(fact, fm.encoding)).getOrElse(s"Dictionary entry '${fact.featureId}' doesn't exist!".failure)

  def validateEncoding(fact: Fact, encoding: Encoding): Validation[String, Fact] = {
    import com.ambiata.ivory.storage.legacy.DictionaryTextStorage.encodingString
    if (fact.value.encoding.exists(_ != encoding))
      s"Not a valid ${encodingString(encoding)}! '${fact.toString}'".failure
    else Success(fact)
  }
}
