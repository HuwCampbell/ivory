package com.ambiata.ivory.core.thrift

import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.TSerializer

case class ThriftSerialiser() {
  val serialiser = new TSerializer(new TCompactProtocol.Factory)
  val deserialiser = new TDeserializerCopy(new TCompactProtocol.Factory)

  def toBytes[A](a: A)(implicit ev: A <:< ThriftLike): Array[Byte] =
    serialiser.serialize(ev(a))

  def fromBytes1[A](empty: () => A, bytes: Array[Byte])(implicit ev: A <:< ThriftLike): A =
    fromBytesUnsafe(empty(), bytes)

  /** WARNING: This mutates the thrift value _in place_ - use with care and only for performance */
  def fromBytesUnsafe[A](empty: A, bytes: Array[Byte])(implicit ev: A <:< ThriftLike): A = {
    fromBytesViewUnsafe(empty, bytes, 0, bytes.length)
  }

  def fromBytesViewUnsafe[A](empty: A, bytes: Array[Byte], offset: Int, length: Int)(implicit ev: A <:< ThriftLike): A = {
    val e = ev(empty)
    e.clear()
    deserialiser.deserialize(e, bytes, offset, length)
    e.asInstanceOf[A]
  }
}
