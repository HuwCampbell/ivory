package com.ambiata.ivory.operation.extraction.reduction

import java.util.{HashMap => JHashMap}

/** Yep, it's just a map. Just in case we want to optimise the internals we can */
class KeyValue[K, V]() {

  val map = new JHashMap[K, V]

  def getOrElse(k: K, v2: V): V = {
    val v = map.get(k)
    if (v == null) v2
    else v
  }

  /** Use with care - only required when calling [[getOrElse()]] with a default value would require call-by-name */
  def getOrNull(k: K): V =
    map.get(k)

  def put(k: K, v: V): Unit = {
    map.put(k, v)
    ()
  }

  /** Just for testing */
  override def equals(other: Any): Boolean =
    other match {
      case that: KeyValue[K, V] => map == that.map
      case _ => false
    }
}
