package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}

class DictionaryOutputSpec extends Specification with ScalaCheck { def is = s2"""
  Dictionary lines can be indexed                                $index
"""

  // This is just a sanity check for matching all the expressions because we can't trust scala exhaustive checking :(
  def index = prop((dict: Dictionary, t: String, delim: Char) => {
    DictionaryOutput.indexedDictionaryLines(dict, t, delim) must haveLength(dict.definitions.length)
  })
}
