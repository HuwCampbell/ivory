package com.ambiata.ivory.extract

import com.ambiata.ivory.core._, Arbitraries._
import org.specs2._
import scalaz.{DList => _}

class ValidateSpec extends Specification with ScalaCheck { def is = s2"""

ValidateSpec
------------

  Can validate with correct encoding                     $valid
  Can validate with incorrect encoding                   $invalid
"""

  def valid = prop((e: EncodingAndValue) =>
    Validate.validateEncoding(e.value, e.enc).toEither must beRight
  )

  def invalid = prop((e: EncodingAndValue, e2: Encoding) => {
    Validate.validateEncoding(e.value, e2).toEither must beLeft
  })

}
