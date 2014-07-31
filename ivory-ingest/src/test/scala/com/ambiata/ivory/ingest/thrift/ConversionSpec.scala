package com.ambiata.ivory.ingest.thrift

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core.Fact
import org.joda.time.DateTimeZone
import org.specs2.{ScalaCheck, Specification}

class ConversionSpec extends Specification with ScalaCheck { def is = s2"""

Conversion
=========
   can convert a fact to and from thrift                     $conversion
   will return an error if the fact is missing               $invalid

"""

  import Conversion._

  val tz = DateTimeZone.getDefault

  def conversion = prop((fact: Fact) => {
    thrift2fact(fact.namespace, fact2thrift(fact), tz, tz).toEither must beRight(fact)
  })

  def invalid =
    thrift2fact("", new ThriftFact, tz, tz).toEither must beLeft
}
