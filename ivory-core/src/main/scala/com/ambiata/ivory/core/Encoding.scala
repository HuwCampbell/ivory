package com.ambiata.ivory.core

import scala.math.{Ordering => SOrdering}

sealed trait Encoding

sealed trait PrimitiveEncoding extends SubEncoding
case object BooleanEncoding   extends PrimitiveEncoding
case object IntEncoding       extends PrimitiveEncoding
case object LongEncoding      extends PrimitiveEncoding
case object DoubleEncoding    extends PrimitiveEncoding
case object StringEncoding    extends PrimitiveEncoding
case object DateEncoding      extends PrimitiveEncoding

sealed trait SubEncoding extends Encoding

case class StructEncoding(values: Map[String, StructEncodedValue]) extends SubEncoding
case class ListEncoding(encoding: SubEncoding) extends Encoding

// NOTE: For now we don't support nested structs
case class StructEncodedValue(encoding: PrimitiveEncoding, optional: Boolean = false) {
  def opt: StructEncodedValue =
    if (optional) this else copy(optional = true)
}

object Encoding {

  def render(enc: Encoding): String = enc match {
    case ListEncoding(e) => "[" + renderSub(e) + "]"
    case e: SubEncoding  => renderSub(e)
  }

  private def renderSub(enc: SubEncoding): String = enc match {
    case e: PrimitiveEncoding => renderPrimitive(e)
    case StructEncoding(m)    => "(" + m.map {
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
    enc match {
      case _: PrimitiveEncoding => true
      case _: StructEncoding    => false
      case _: ListEncoding      => false
    }
}
