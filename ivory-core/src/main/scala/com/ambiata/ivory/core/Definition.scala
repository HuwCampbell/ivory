package com.ambiata.ivory.core

sealed trait Definition {
  def fold[X](
    concrete: (FeatureId, ConcreteDefinition) => X
  , virtual: (FeatureId, VirtualDefinition) => X
  ): X = this match {
    case Concrete(featureId, definition) => concrete(featureId, definition)
    case Virtual(featureId, definition) => virtual(featureId, definition)
  }

  def featureId: FeatureId =
    fold((f, _) => f, (f, _) => f)

  def featureId_=(featureId: FeatureId): Definition =
    fold((_, d) => Concrete(featureId, d), (_, d) => Virtual(featureId, d))
}

case class Concrete(id: FeatureId, definition: ConcreteDefinition) extends Definition
case class Virtual(id: FeatureId, definition: VirtualDefinition) extends Definition

object Definition {
  def concrete(featureId: FeatureId, encoding: Encoding, ty: Option[Type], desc: String, tombstoneValue: List[String]): Definition =
    Concrete(featureId, ConcreteDefinition(encoding, ty, desc, tombstoneValue))

  def virtual(featureId: FeatureId, alias: FeatureId, window: Option[Window]): Definition =
    Virtual(featureId, VirtualDefinition(alias, window))
}

case class ConcreteDefinition(encoding: Encoding, ty: Option[Type], desc: String, tombstoneValue: List[String]) {
  def toDefinition(featureId: FeatureId): Definition =
    Concrete(featureId, this)
}
case class VirtualDefinition(alias: FeatureId, window: Option[Window]) {
  def toDefinition(featureId: FeatureId): Definition =
    Virtual(featureId, this)
}
