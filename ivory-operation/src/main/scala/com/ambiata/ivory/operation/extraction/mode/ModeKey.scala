package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFactPrimitiveValue
import com.ambiata.ivory.storage.lookup.FeatureLookups
import com.ambiata.mundane.bytes.Buffer
import com.ambiata.poacher.mr.ByteWriter

import org.apache.hadoop.io.WritableComparator

import scalaz._, Scalaz._


case class ModeKey(append: (Fact, Buffer) => Buffer)

object ModeKey {

  /** This is convenient wrapper around [[fromLookup]], but we don't actually need everything in the dictionary */
  def fromDictionary(dictionary: Dictionary): Array[ModeKey] =
    fromLookup(toModeAndEncoding(dictionary))

  def toModeAndEncoding(dictionary: Dictionary): Map[Int, (Mode, Encoding)] =
    dictionary.byFeatureIndex.flatMap({
      case (i, d) =>
        d.fold((_, cd) => Some(i -> (cd.mode -> cd.encoding)), (_, _) => None)
    })

  def fromLookup(definitions: Map[Int, (Mode, Encoding)]): Array[ModeKey] =
    FeatureLookups.sparseMapToArray(definitions.traverseU((fromDefinition _).tupled)
      .fold(Crash.error(Crash.Invariant, _), identity).toList, blank)

  def fromDefinition(mode: Mode, encoding: Encoding): String \/ ModeKey = {
    mode.fold(
      blank.right,
      blank.right,
      key => fromEncoding(key, encoding)
    )
  }

  val blank: ModeKey =
    ModeKey((_, b) => b)

  def const(bytes: Array[Byte]): ModeKey =
    ModeKey((_, b) => {
      val b2 = Buffer.allocate(b, bytes.length)
      ByteWriter.write(b2.bytes, bytes, b2.offset)
      b2
    })

  def fromEncoding(key: String, encoding: Encoding): String \/ ModeKey =
    encoding.fold(
      _ => "Invalid primitive encoding for keyed_set".left,
      s => fromStruct(key, s),
      _ => "Invalid list encoding for keyed_set".left
    )

  def fromStruct(key: String, encoding: StructEncoding): String \/ ModeKey =
    encoding.values.get(key)
      .toRightDisjunction(s"Missing required keyed_set struct field: $key")
      .map(pe => struct(key, valueBytes(pe.encoding)))

  def struct(key: String, toBytes: (ThriftFactPrimitiveValue, Buffer) => Buffer): ModeKey =
    ModeKey({
      (f, b) =>
        val value = f.toThrift.getValue
        if (value.isSetT) {
          val b2 = Buffer.allocate(b, 1)
          ByteWriter.writeByte(b2.bytes, Byte.MinValue, b2.offset)
          b2
        } else {
          if (!value.isSetStructSparse)
            Crash.error(Crash.DataIntegrity, s"Fact '${f.entity}' is not a struct required by a keyed_set")
          val v = value.getStructSparse.v.get(key)
          if (v == null)
            Crash.error(Crash.DataIntegrity, s"Fact '${f.entity}' is missing the struct field '$key'")
          toBytes(v, b)
        }
    })

  def valueBytes(encoding: PrimitiveEncoding): (ThriftFactPrimitiveValue, Buffer) => Buffer =
    encoding match {
      case StringEncoding  =>
        (v, b) =>
          val bytes = v.getS.getBytes("UTF-8")
          val b2 = Buffer.allocate(b, bytes.length)
          System.arraycopy(bytes, 0, b2.bytes, b2.offset, bytes.length)
          b2
      case BooleanEncoding =>
        (v, b) =>
          val b2 = Buffer.allocate(b, 1)
          b2.bytes(b2.offset) = if (v.getB) 1 else 0
          b2
      case IntEncoding =>
        (v, b) =>
          val b2 = Buffer.allocate(b, 4)
          ByteWriter.writeInt(b2.bytes, v.getI, b2.offset)
          b2
      case LongEncoding =>
        (v, b) =>
          val b2 = Buffer.allocate(b, 8)
          ByteWriter.writeLong(b2.bytes, v.getL, b2.offset)
          b2
      case DoubleEncoding =>
        (v, b) =>
          val b2 = Buffer.allocate(b, 8)
          ByteWriter.writeLong(b2.bytes, java.lang.Double.doubleToLongBits(v.getD), b2.offset)
          b2
      case DateEncoding =>
        (v, b) =>
          val b2 = Buffer.allocate(b, 4)
          ByteWriter.writeInt(b2.bytes, v.getDate, b2.offset)
          b2
    }

  def byteValue(encoding: PrimitiveEncoding): Buffer => ThriftFactPrimitiveValue =
    encoding match {
      case StringEncoding =>
        v => ThriftFactPrimitiveValue.s(new String(v.bytes, v.offset, v.length, "UTF-8"))
      case BooleanEncoding =>
        v => ThriftFactPrimitiveValue.b(v.bytes(v.offset) == 1)
      case IntEncoding =>
        v => ThriftFactPrimitiveValue.i(WritableComparator.readInt(v.bytes, v.offset))
      case LongEncoding =>
        v => ThriftFactPrimitiveValue.l(WritableComparator.readLong(v.bytes, v.offset))
      case DoubleEncoding =>
        v => ThriftFactPrimitiveValue.d(WritableComparator.readDouble(v.bytes, v.offset))
      case DateEncoding =>
        v => ThriftFactPrimitiveValue.date(WritableComparator.readInt(v.bytes, v.offset))
    }
}
