package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Fact
import com.ambiata.ivory.core.thrift._
import scala.collection.JavaConverters._ 

case class LatestByReducer(key: String) extends Reduction {

  var keyValue: KeyValue[String, ThriftFactValue] = new KeyValue[String, ThriftFactValue]
  
  def clear(): Unit = {
    keyValue = new KeyValue[String, ThriftFactValue]
  }

  def update(fv: Fact): Unit = {
    if (!fv.isTombstone) {
      val factKey = fv.toThrift.getValue.getStructSparse.getV.get(key)
      if (factKey != null && factKey.getS != null) {
        val value = fv.toThrift.getValue
        keyValue.put(factKey.getS, value)
      }
    }
  }

  def skip(f: Fact, reason: String): Unit = ()

  def save: ThriftFactValue = {
    val xs = keyValue.map.asScala.values 

    ThriftFactValue.lst(new ThriftFactList(xs.map {
        tv => ThriftFactListValue.s(tv.getStructSparse)
    }.toList.asJava))
  }
}
