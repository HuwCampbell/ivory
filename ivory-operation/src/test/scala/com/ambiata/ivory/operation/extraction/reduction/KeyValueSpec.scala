package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class KeyValueSpec extends Specification with ScalaCheck { def is = s2"""

  Getting from a map with a value returns that value          $getOrElsePut
  Getting from an empty map returns the default value         $getOrElseDefault
  Getting from a map with a value returns that value          $getOrElseNullPut
  Getting from an empty map returns null                      $getOrElseNullDefault
  Putting two values will key the second value                $put
"""

  def getOrElsePut = prop((s1: String, s2: String, s3: String) => {
    val kv = new KeyValue[String, String]
    kv.put(s1, s2)
    kv.getOrElse(s1, s3) ==== s2
  })

  def getOrElseDefault = prop((s1: String, s2: String) => {
    val kv = new KeyValue[String, String]
    kv.getOrElse(s1, s2) ==== s2
  })

  def getOrElseNullPut = prop((s1: String, s2: String) => {
    val kv = new KeyValue[String, String]
    kv.put(s1, s2)
    kv.getOrNull(s1) must not beNull
  })

  def getOrElseNullDefault = prop((s1: String, s2: String) => {
    val kv = new KeyValue[String, String]
    kv.getOrNull(s1) must beNull
  })

  def put = prop((s1: String, s2: String, s3: String, s4: String) => {
    val kv = new KeyValue[String, String]
    kv.put(s1, s2)
    kv.put(s1, s3)
    kv.getOrElse(s1, s4) ==== s3
  })
}
