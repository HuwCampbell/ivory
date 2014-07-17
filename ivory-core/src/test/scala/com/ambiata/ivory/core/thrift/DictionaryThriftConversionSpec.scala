package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core._, Arbitraries._
import org.specs2.{ScalaCheck, Specification}

class DictionaryThriftConversionSpec extends Specification with ScalaCheck { def is = s2"""

Dictionary Thrift
-----------------

  Conversion                                       $conversion

"""

  import DictionaryThriftConversion.dictionary._

  def conversion = prop((dict: Dictionary) => from(to(dict)) ==== dict)
}
