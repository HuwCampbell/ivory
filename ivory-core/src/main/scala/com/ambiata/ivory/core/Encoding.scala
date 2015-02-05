package com.ambiata.ivory.core

/**
 * Represents the type of encodings possible for a given feature.
 *
 * The hierarchy looks like:
 *
 * ```
 * 
 *                  TopLevelEncoding
 *                        o
 *                        |
 *              --------------------
 *              |         |        |
 *              o         o        o
 *  StructTopLevelEncoding(SE) PrimitiveTopLevelEncoding(PE)        
 *               ListTopLevelEncoding(LE)
 * 
 * 
 *                  NestedEncoding
 *                        o
 *                        |
 *              --------------------
 *              |                  |
 *              o                  o
 *  NestedStructEncoding(SE) NestedPrimitiveEncoding(PE)
 *
 *                 PrimitiveEncoding (PE)
 *                       /|\
 *                {stirng,int,....}
 * 
 *             ListEncoding([NestedEncoding]) (LE)
 * 
 *         StructEncoding({name : NestedEncoding}) (SE)
 * 
 * Encoding
 * --------
 *  - EncodingList
 *    * ListEncoding(SubEncoding)
 *  - EncodingSub
 *    * SubEncoding
 *      - SubStruct
 *        * StructEncoding(PrimitiveEncoding)
 *      - SubPrimitive
 *        * PrimitiveEncoding
 * ```
 */
sealed trait Encoding {

  def fold[A](p: PrimitiveEncoding => A, s: StructEncoding => A, l: ListEncoding => A): A = this match {
    case EncodingList(e) => l(e)
    case EncodingSub(SubStruct(e)) => s(e)
    case EncodingSub(SubPrim(e)) => p(e)
  }

  def foldRec[A](p: PrimitiveEncoding => A, s: Map[String, StructEncodedValue[A]] => A, l: A => A): A = {
    def foldRecSub(se: SubEncoding): A = se match {
      case SubStruct(e) => s(e.values.mapValues(_.map(p)))
      case SubPrim(e) => p(e)
    }
    this match {
      case EncodingList(e) => l(foldRecSub(e.encoding))
      case EncodingSub(e) => foldRecSub(e)
    }
  }
}
case class EncodingSub(e: SubEncoding) extends Encoding
case class EncodingList(encoding: ListEncoding) extends Encoding

sealed trait PrimitiveEncoding {

  def toEncoding: Encoding =
    SubPrim(this).toEncoding
}
case object BooleanEncoding   extends PrimitiveEncoding
case object IntEncoding       extends PrimitiveEncoding
case object LongEncoding      extends PrimitiveEncoding
case object DoubleEncoding    extends PrimitiveEncoding
case object StringEncoding    extends PrimitiveEncoding
case object DateEncoding      extends PrimitiveEncoding

sealed trait SubEncoding {

  def toEncoding: Encoding =
    EncodingSub(this)
}
case class SubPrim(e: PrimitiveEncoding) extends SubEncoding
case class SubStruct(e: StructEncoding) extends SubEncoding

case class StructEncoding(values: Map[String, StructEncodedValue[PrimitiveEncoding]]) {

  def toEncoding: Encoding =
    SubStruct(this).toEncoding
}
case class ListEncoding(encoding: SubEncoding) {

  def toEncoding: Encoding =
    EncodingList(this)
}

// NOTE: For now we don't support nested structs
case class StructEncodedValue[A](encoding: A, optional: Boolean) {
  def opt: StructEncodedValue[A] =
    if (optional) this else copy(optional = true)

  def map[B](f: A => B): StructEncodedValue[B] =
    StructEncodedValue(f(encoding), optional)
}

object StructEncodedValue {

  def optional(encoding: PrimitiveEncoding): StructEncodedValue[PrimitiveEncoding] =
    StructEncodedValue(encoding, optional = true)

  def mandatory(encoding: PrimitiveEncoding): StructEncodedValue[PrimitiveEncoding] =
    StructEncodedValue(encoding, optional = false)
}

object Encoding {

  def render(enc: Encoding): String = enc.foldRec[String](
    renderPrimitive,
    m =>
      "(" + m.map {
        case (n, v) => n + ":" + v.encoding + (if (v.optional) "*" else "")
      }.mkString(",") + ")",
    e =>
      "[" + e + "]"
  )

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
