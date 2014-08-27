package com.ambiata.ivory.core

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
