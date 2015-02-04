package com.ambiata.ivory.core

sealed trait Encoding {

  def fold[A](p: PrimitiveEncoding => A, s: StructEncoding => A, l: ListEncoding => A): A = this match {
    case EncodingPrim(e) => p(e)
    case EncodingStruct(e) => s(e)
    case EncodingList(e) => l(e)
  }
}
case class EncodingPrim(encoding: PrimitiveEncoding) extends Encoding
case class EncodingStruct(encoding: StructEncoding) extends Encoding
case class EncodingList(encoding: ListEncoding) extends Encoding

sealed trait PrimitiveEncoding {

  def toEncoding: Encoding =
    EncodingPrim(this)
}
case object BooleanEncoding   extends PrimitiveEncoding
case object IntEncoding       extends PrimitiveEncoding
case object LongEncoding      extends PrimitiveEncoding
case object DoubleEncoding    extends PrimitiveEncoding
case object StringEncoding    extends PrimitiveEncoding
case object DateEncoding      extends PrimitiveEncoding

sealed trait SubEncoding {

  def fold[A](p: PrimitiveEncoding => A, s: StructEncoding => A): A = this match {
    case SubPrim(e) => p(e)
    case SubStruct(e) => s(e)
  }

  def toEncoding: Encoding =
    fold(_.toEncoding, _.toEncoding)
}
case class SubPrim(e: PrimitiveEncoding) extends SubEncoding
case class SubStruct(e: StructEncoding) extends SubEncoding

case class StructEncoding(values: Map[String, StructEncodedValue]) {

  def toEncoding: Encoding =
    EncodingStruct(this)
}
case class ListEncoding(encoding: SubEncoding) {

  def toEncoding: Encoding =
    EncodingList(this)
}

// NOTE: For now we don't support nested structs
case class StructEncodedValue(encoding: PrimitiveEncoding, optional: Boolean = false) {
  def opt: StructEncodedValue =
    if (optional) this else copy(optional = true)
}

object StructEncodedValue {

  def optional(encoding: PrimitiveEncoding): StructEncodedValue =
    StructEncodedValue(encoding, optional = true)

  def mandatory(encoding: PrimitiveEncoding): StructEncodedValue =
    StructEncodedValue(encoding, optional = false)
}

object Encoding {

  def render(enc: Encoding): String = enc match {
    case EncodingList(ListEncoding(e)) => "[" + renderSub(e) + "]"
    case EncodingSub(e) => renderSub(e)
  }

  private def renderSub(enc: SubEncoding): String = enc match {
    case SubPrim(e) => renderPrimitive(e)
    case SubStruct(m)    => "(" + m.values.map {
      case (n, v) => n + ":" + renderPrimitive(v.encoding) + (if (v.optional) "*" else "")
    }.mkString(",") + ")"
  }

  def renderPrimitive(enc: PrimitiveEncoding): String = enc match {
    case BooleanEncoding => "boolean"
    case IntEncoding     => "int"
    case LongEncoding    => "long"
    case DoubleEncoding  => "double"
    case StringEncoding  => "string"
    case DateEncoding    => "date"
  }

  def isPrimitive(enc: Encoding): Boolean =
    enc.fold(_ => true, _ => false, _ => false)

  def isNumeric(enc: Encoding): Boolean =
    enc.fold(isNumericPrim, _ => false, _ => false)

  def isNumericPrim(enc: PrimitiveEncoding): Boolean =
    enc match {
      case StringEncoding    => false
      case IntEncoding       => true
      case LongEncoding      => true
      case DoubleEncoding    => true
      case BooleanEncoding   => false
      case DateEncoding      => false
    }
}
