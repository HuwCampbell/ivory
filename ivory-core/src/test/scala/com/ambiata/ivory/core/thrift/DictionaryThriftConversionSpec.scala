package com.ambiata.ivory.core.thrift

import com.ambiata.ivory.core._, Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import DictionaryThriftConversion._

class DictionaryThriftConversionSpec extends Specification with ScalaCheck { def is = s2"""

Dictionary Thrift
-----------------

  Conversion                                       $conversion

"""

  def conversion = prop { dictionary: Dictionary =>
    dictionaryFromThrift(dictionaryToThrift(dictionary)).toEither must beRight(dictionary)
  }
}
