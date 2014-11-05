package com.ambiata.ivory.mr

import com.ambiata.mundane.io.{MemoryConversions, BytesQuantity}
import com.ambiata.poacher.mr.ThriftSerialiser
import com.nicta.scoobi.Scoobi.WireFormat
import com.nicta.scoobi.Scoobi._
import scalaz.{Name => _, DList => _, Value => _, _}, Scalaz._
import java.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._

import MemoryConversions._

trait WireFormats {

  /**
   * TODO Remove this when scoobi has the wire format
   */
  def featureIdWireFormat =
    implicitly[WireFormat[(String, String)]].xmap(
      (nsn: (String, String)) => FeatureId(Name.unsafe(nsn._1), nsn._2),
      (x: FeatureId) => (x.namespace.name, x.name))

  /**
   * TODO Remove this when scoobi has the wire format
   */
  implicit val ShortWireFormat = new WireFormat[Short] {
    def toWire(x: Short, out: DataOutput) { out.writeShort(x) }
    def fromWire(in: DataInput): Short = in.readShort()
    override def toString = "Short"
  }

  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  /** this is a special snowflake because you want to mix in Fact without the overhead of creating two objects. */
  def factWireFormat = new WireFormat[Fact] {

    def toWire(x: Fact, out: DataOutput) = {
      val serialiser = ThriftSerialiser()
      val bytes = serialiser.toBytes(x.toNamespacedThrift)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
    def fromWire(in: DataInput): Fact = {
      val serialiser = ThriftSerialiser()
      val size = in.readInt()
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
      serialiser.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)
    }
  }

  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def parseErrorWireFormat = new WireFormat[ParseError] {

    def toWire(x: ParseError, out: DataOutput) = {
      val serialiser = ThriftSerialiser()
      val bytes = serialiser.toBytes(x.toThrift)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
    def fromWire(in: DataInput): ParseError = {
      val serialiser = ThriftSerialiser()
      val size = in.readInt()
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
      val e = serialiser.fromBytesUnsafe(new ThriftParseError(), bytes)
      ParseError.fromThrift(e)
    }

  }

  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def namespacedThriftFactWireFormat = new WireFormat[NamespacedThriftFact] {
    val x = mkThriftFmt(new NamespacedThriftFact)
    def toWire(tf: NamespacedThriftFact, out: DataOutput) =  x.toWire(tf, out)
    def fromWire(in: DataInput): NamespacedThriftFact = x.fromWire(in)
  }

  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  def thriftFactWireFormat = new WireFormat[ThriftFact] {
    val x = mkThriftFmt(new ThriftFact)
    def toWire(tf: ThriftFact, out: DataOutput) = x.toWire(tf, out)
    def fromWire(in: DataInput): ThriftFact = x.fromWire(in)
  }

  implicit def ValidationWireFormat[A, B](implicit awf: WireFormat[A], bwf: WireFormat[B]) = new WireFormat[Validation[A, B]] {
    def toWire(v: Validation[A, B], out: DataOutput) = {
      v match {
        case Failure(a) => { out.writeBoolean(false); awf.toWire(a, out) }
        case Success(b) => { out.writeBoolean(true); bwf.toWire(b, out) }
      }
    }

    def fromWire(in: DataInput): Validation[A, B] = {
      in.readBoolean match {
        case false => awf.fromWire(in).failure
        case true  => bwf.fromWire(in).success
      }
    }

    def show(v: Validation[A, B]): String = v.toString
  }

  implicit def DateMapWireFormat = AnythingFmt[java.util.HashMap[String, Array[Int]]]

  /** WARNING THIS IS NOT SAFE TO EXPOSE, DANGER LURKS, SEE ThriftFactWireFormat */
  private def mkThriftFmt[A](empty: A)(implicit ev: A <:< ThriftLike): WireFormat[A] = new WireFormat[A] {
    val serialiser = ThriftSerialiser()
    def toWire(x: A, out: DataOutput) = {
      val bytes = serialiser.toBytes(x)
      out.writeInt(bytes.length)
      out.write(bytes)
    }
    def fromWire(in: DataInput): A = {
      val size = in.readInt()
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
      serialiser.fromBytesUnsafe(empty.deepCopy().asInstanceOf[A], bytes)
    }
    override def toString = "ThriftObject"
  }

  implicit def BytesQuantityWireFormat: WireFormat[BytesQuantity] =
    com.nicta.scoobi.core.WireFormat.LongFmt.xmap[BytesQuantity](_.bytes, _.toBytes.value)
}

object WireFormats extends WireFormats
