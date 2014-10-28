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
  def concrete(featureId: FeatureId, encoding: Encoding, mode: Mode, ty: Option[Type], desc: String, tombstoneValue: List[String]): Definition =
    Concrete(featureId, ConcreteDefinition(encoding, mode, ty, desc, tombstoneValue))

  def virtual(featureId: FeatureId, source: FeatureId, query: Query, window: Option[Window]): Definition =
    Virtual(featureId, VirtualDefinition(source, query, window))
}

case class ConcreteDefinition(encoding: Encoding, mode: Mode, ty: Option[Type], desc: String, tombstoneValue: List[String]) {
  def toDefinition(featureId: FeatureId): Definition =
    Concrete(featureId, this)
}
case class VirtualDefinition(source: FeatureId, query: Query, window: Option[Window]) {
  def toDefinition(featureId: FeatureId): Definition =
    Virtual(featureId, this)
}
