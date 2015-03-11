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
      keys => fromEncoding(keys, encoding)
    )
  }

  val blank: ModeKey =
    ModeKey((_, b) => b)

  def const(bytes: Array[Byte]): ModeKey =
    ModeKey((_, b) => {
      val b2 = Buffer.grow(b, bytes.length)
      ByteWriter.write(b2.bytes, bytes, b2.offset)
      b2
    })

  def fromEncoding(keys: List[String], encoding: Encoding): String \/ ModeKey =
    encoding.fold(
      _ => "Invalid primitive encoding for keyed_set".left,
      s => fromStruct(keys, s),
      _ => "Invalid list encoding for keyed_set".left
    )

  def fromStruct(keys: List[String], encoding: StructEncoding): String \/ ModeKey =
    keys.traverseU(key => encoding.values.get(key)
      .toRightDisjunction(s"Missing required keyed_set struct field: $key")
      .map(pe => struct(key, valueBytes(pe.encoding)))).map(structs)

  def structs(keys: List[(Fact, Buffer, Int) => Buffer]): ModeKey =
    ModeKey({
      (f, b) =>
        var b2 = b
        // Saving that one, stupid extra allocation
        var _keys = keys
        var offset = 0
        while (!_keys.isEmpty) {
          val oldLength = b2.length
          b2 = _keys.head(f, b2, offset)
          offset += b2.length - oldLength
          _keys = _keys.tail
        }
        b2
    })

  def struct(key: String, toBytes: (ThriftFactPrimitiveValue, Buffer, Int) => Buffer): (Fact, Buffer, Int) => Buffer = {
    (f, b, i) =>
      val value = f.toThrift.getValue
      if (value.isSetT) {
        val b2 = Buffer.grow(b, 1)
        ByteWriter.writeByte(b2.bytes, Byte.MinValue, b2.offset + i)
        b2
      } else {
        if (!value.isSetStructSparse)
          Crash.error(Crash.DataIntegrity, s"Fact '${f.entity}' is not a struct required by a keyed_set")
        val v = value.getStructSparse.v.get(key)
        if (v == null)
          Crash.error(Crash.DataIntegrity, s"Fact '${f.entity}' is missing the struct field '$key'")
        toBytes(v, b, i)
      }
  }

  def valueBytes(encoding: PrimitiveEncoding): (ThriftFactPrimitiveValue, Buffer, Int) => Buffer =
    encoding match {
      case StringEncoding  =>
        (v, b, o) =>
          val bytes = v.getS.getBytes("UTF-8")
          val b2 = Buffer.grow(b, bytes.length)
          System.arraycopy(bytes, 0, b2.bytes, b2.offset + o, bytes.length)
          b2
      case BooleanEncoding =>
        (v, b, o) =>
          val b2 = Buffer.grow(b, 1)
          b2.bytes(b2.offset + o) = if (v.getB) 1 else 0
          b2
      case IntEncoding =>
        (v, b, o) =>
          val b2 = Buffer.grow(b, 4)
          ByteWriter.writeInt(b2.bytes, v.getI, b2.offset + o)
          b2
      case LongEncoding =>
        (v, b, o) =>
          val b2 = Buffer.grow(b, 8)
          ByteWriter.writeLong(b2.bytes, v.getL, b2.offset + o)
          b2
      case DoubleEncoding =>
        (v, b, o) =>
          val b2 = Buffer.grow(b, 8)
          ByteWriter.writeLong(b2.bytes, java.lang.Double.doubleToLongBits(v.getD), b2.offset + o)
          b2
      case DateEncoding =>
        (v, b, o) =>
          val b2 = Buffer.grow(b, 4)
          ByteWriter.writeInt(b2.bytes, v.getDate, b2.offset + o)
          b2
    }

  def byteValue(encoding: PrimitiveEncoding): (Buffer, Int) => ThriftFactPrimitiveValue =
    encoding match {
      case StringEncoding =>
        (v, o) => ThriftFactPrimitiveValue.s(new String(v.bytes, v.offset + o, v.length, "UTF-8"))
      case BooleanEncoding =>
        (v, o) => ThriftFactPrimitiveValue.b(v.bytes(v.offset + o) == 1)
      case IntEncoding =>
        (v, o) => ThriftFactPrimitiveValue.i(WritableComparator.readInt(v.bytes, v.offset + o))
      case LongEncoding =>
        (v, o) => ThriftFactPrimitiveValue.l(WritableComparator.readLong(v.bytes, v.offset + o))
      case DoubleEncoding =>
        (v, o) => ThriftFactPrimitiveValue.d(WritableComparator.readDouble(v.bytes, v.offset + o))
      case DateEncoding =>
        (v, o) => ThriftFactPrimitiveValue.date(WritableComparator.readInt(v.bytes, v.offset + o))
    }
}
