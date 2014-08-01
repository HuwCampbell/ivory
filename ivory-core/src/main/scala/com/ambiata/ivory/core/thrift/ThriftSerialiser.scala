package com.ambiata.ivory.core.thrift

import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TSerializer, TDeserializer}

case class ThriftSerialiser() {
  val serialiser = new TSerializer(new TCompactProtocol.Factory)
  val deserialiser = new TDeserializer(new TCompactProtocol.Factory)

  def toBytes[A](a: A)(implicit ev: A <:< ThriftLike): Array[Byte] =
    serialiser.serialize(ev(a))

  def fromBytes[A](empty: A, bytes: Array[Byte])(implicit ev: A <:< ThriftLike): A =
    fromBytesUnsafe(ev(empty).deepCopy.asInstanceOf[A], bytes)

  def fromBytes1[A](empty: () => A, bytes: Array[Byte])(implicit ev: A <:< ThriftLike): A =
    fromBytesUnsafe(empty(), bytes)

  /* WARNING: This mutates the thrift value _in place_ - use with care and only for performance */
  def fromBytesUnsafe[A](empty: A, bytes: Array[Byte])(implicit ev: A <:< ThriftLike): A = {
    val e = ev(empty)
    e.clear()
    deserialiser.deserialize(e, bytes)
    e.asInstanceOf[A]
  }

  def fromBytesViewUnsafe[A](empty: A, bytes: Array[Byte], offset: Int, length: Int)(implicit ev: A <:< ThriftLike): A = {
    fromBytesUnsafe(empty, if (offset == 0 && bytes.length == length) bytes else {
      val result = new Array[Byte](length)
      // TODO We need our own version of TDeserializer that uses the offset/length correctly
      System.arraycopy(bytes, offset, result, 0, length)
      result
    })
  }
}
