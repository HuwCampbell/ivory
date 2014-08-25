package com.ambiata.ivory.core

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}

/** The feature dictionary is simply a look up of metadata for a given identifier/name. */
case class Dictionary(meta: Map[FeatureId, Definition]) {

  /** Create a `Dictionary` from `this` only containing features in the specified namespace. */
  def forNamespace(namespace: Name): Dictionary =
    Dictionary(meta filter { case (fid, _) => fid.namespace === namespace })

  /** Create a `Dictionary` from `this` only containing the specified features. */
  def forFeatureIds(featureIds: Set[FeatureId]): Dictionary =
    Dictionary(meta filter { case (fid, _) => featureIds.contains(fid) })

  /** append the mappings coming from another dictionary */
  def append(other: Dictionary) =
    Dictionary(meta ++ other.meta)

}

case class FeatureId(namespace: Name, name: String) {
  override def toString =
    toString(":")

  def toString(delim: String): String =
    s"${namespace.name}${delim}${name}"
}

object FeatureId {
  implicit val orderingByNamespace: SOrdering[FeatureId] =
    SOrdering.by(f => (f.namespace.name, f.name))
}

sealed trait Definition
case class Concrete(definition: ConcreteDefinition) extends Definition
case class Virtual() extends Definition

object Concrete {
  def apply(encoding: Encoding, ty: Option[Type], desc: String, tombstoneValue: List[String]): Definition =
    Concrete(ConcreteDefinition(encoding, ty, desc, tombstoneValue))
}

case class ConcreteDefinition(encoding: Encoding, ty: Option[Type], desc: String, tombstoneValue: List[String]) {
  def definition: Definition = Concrete(this)
}

sealed trait Encoding

sealed trait PrimitiveEncoding extends SubEncoding
case object BooleanEncoding   extends PrimitiveEncoding
case object IntEncoding       extends PrimitiveEncoding
case object LongEncoding      extends PrimitiveEncoding
case object DoubleEncoding    extends PrimitiveEncoding
case object StringEncoding    extends PrimitiveEncoding

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
  }

  def isPrimitive(enc: Encoding): Boolean =
    enc match {
      case _: PrimitiveEncoding => true
      case _: StructEncoding    => false
      case _: ListEncoding      => false
    }
}

sealed trait Type
case object NumericalType   extends Type
case object ContinuousType  extends Type
case object CategoricalType extends Type
case object BinaryType      extends Type

object Type {
  def render(ty: Type): String = ty match {
    case NumericalType   => "numerical"
    case ContinuousType  => "continuous"
    case CategoricalType => "categorical"
    case BinaryType      => "binary"
  }
}
