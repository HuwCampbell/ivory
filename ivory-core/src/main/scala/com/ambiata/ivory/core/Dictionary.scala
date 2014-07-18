package com.ambiata.ivory.core

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}

/** The feature dictionary is simply a look up of metadata for a given identifier/name. */
case class Dictionary(meta: Map[FeatureId, FeatureMeta]) {

  /** Create a `Dictionary` from `this` only containing features in the specified namespace. */
  def forNamespace(namespace: String): Dictionary =
    Dictionary(meta filter { case (fid, _) => fid.namespace === namespace })

  /** Create a `Dictionary` from `this` only containing the specified features. */
  def forFeatureIds(featureIds: Set[FeatureId]): Dictionary =
    Dictionary(meta filter { case (fid, _) => featureIds.contains(fid) })

  /** append the mappings coming from another dictionary */
  def append(other: Dictionary) =
    Dictionary(meta ++ other.meta)

}

case class FeatureId(namespace: String, name: String) {
  override def toString =
    toString(":")

  def toString(delim: String): String =
    s"${namespace}${delim}${name}"
}

object FeatureId {
  implicit val orderingByNamespace: SOrdering[FeatureId] =
    SOrdering.by(f => (f.namespace, f.name))
}

case class FeatureMeta(encoding: Encoding, ty: Option[Type], desc: String, tombstoneValue: List[String] = List("☠")) {
  def toString(delim: String): String =
    s"${Encoding.render(encoding)}${delim}${ty.map(Type.render).getOrElse("")}${delim}${desc}${delim}${tombstoneValue.mkString(",")}"
}

sealed trait Encoding

sealed trait PrimitiveEncoding extends SubEncoding
case object BooleanEncoding   extends PrimitiveEncoding
case object IntEncoding       extends PrimitiveEncoding
case object LongEncoding      extends PrimitiveEncoding
case object DoubleEncoding    extends PrimitiveEncoding
case object StringEncoding    extends PrimitiveEncoding

sealed trait SubEncoding extends Encoding

case class StructEncoding(values: Map[String, StructValue]) extends SubEncoding

// NOTE: For now we don't support nested structs
case class StructValue(encoding: PrimitiveEncoding, optional: Boolean = false) {
  def opt: StructValue =
    if (optional) this else copy(optional = true)
}

object Encoding {

  def render(enc: Encoding): String = enc match {
    case e: PrimitiveEncoding => renderPrimitive(e)
    case _: StructEncoding    => sys.error("Encoding of structs not supported yet!") // TODO
  }

  def renderPrimitive(enc: PrimitiveEncoding): String = enc match {
    case BooleanEncoding => "boolean"
    case IntEncoding     => "int"
    case LongEncoding    => "long"
    case DoubleEncoding  => "double"
    case StringEncoding  => "string"
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
