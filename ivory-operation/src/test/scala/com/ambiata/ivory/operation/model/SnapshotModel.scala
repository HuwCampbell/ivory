package com.ambiata.ivory.operation.model

import com.ambiata.ivory.core._

import scalaz.{Value => _, _}, Scalaz._


case class SnapshotModelConf(date: Date, dictionary: Dictionary)

object SnapshotModel extends MapReduceSimple[Prioritized[Fact], Fact, SnapshotModelConf] {
  type M = Fact
  type K = (String, FeatureId)
  type S = (DateTime, List[PrimitiveValue], Priority)

  def map(facts: List[Prioritized[Fact]], conf: SnapshotModelConf): List[(K, S, Fact)] =
    facts
      .filter(_.value.date <= conf.date)
      .map(pf => mapKeyedDict(conf.dictionary, pf))

  def mapKeyedDict(dict: Dictionary, pf: Prioritized[Fact]): (K, S, Fact) =
    mapKeyed(dict.byConcrete.sources.getOrElse(pf.value.featureId, sys.error(s"Unknown feature for $pf")).definition, pf)

  def mapKeyed(cd: ConcreteDefinition, pf: Prioritized[Fact]): (K, S, Fact) = {
    val f = pf.value
    val v = cd.mode.fold(Nil, Nil, keys => f.value match {
      case StructValue(m) =>
        keys.map(m.get).flatten
      case _ =>
        sys.error(s"Non-struct value for keyed_set fact: $f")
    })
    ((f.entity, f.featureId), (f.datetime, v, pf.priority), f)
  }

  def secondary: Order[S] =
    Order.order {
      case ((dt1, v1, p1), (dt2, v2, p2)) =>
        dt1.long ?|? dt2.long |+|
          listOrder(Value.orderPrimitive)(v1, v2) |+|
          p1 ?|? p2
    }

  def reduce(key: K, facts: NonEmptyList[Fact], conf: SnapshotModelConf): List[Fact] =
    reduceWithFeature(facts, conf.dictionary.byConcrete.sources(key._2), conf.date)

  def reduceWithFeature(facts: NonEmptyList[Fact], dictionary: ConcreteGroup, date: Date): List[Fact] =
    ModeHandler.get(dictionary.definition.mode).reduce(facts, dictionary.range(date).fromOrMax)
}
