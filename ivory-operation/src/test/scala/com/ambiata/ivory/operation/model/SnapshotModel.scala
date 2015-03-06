package com.ambiata.ivory.operation.model

import com.ambiata.ivory.core._

import scalaz.{Value => _, _}, Scalaz._


case class SnapshotModelConf(date: Date, dictionary: Dictionary)

object SnapshotModel extends MapReduceSimple[Prioritized[Fact], Fact, SnapshotModelConf] {
  type M = Fact
  type K = (String, FeatureId)
  type S = (DateTime, Priority)

  def map(facts: List[Prioritized[Fact]], conf: SnapshotModelConf): List[(K, S, Fact)] =
    facts
      .filter(_.value.date <= conf.date)
      .map(mapKeyed)

  def mapKeyed(pf: Prioritized[Fact]): (K, S, Fact) = {
    val f = pf.value
    ((f.entity, f.featureId), (f.datetime, pf.priority), f)
  }

  def secondary: Order[S] =
    Order.orderBy {
      case (dt, p) => (dt.long, p)
    }

  def reduce(key: K, facts: NonEmptyList[Fact], conf: SnapshotModelConf): List[Fact] =
    reduceWithFeature(facts, conf.dictionary.byConcrete.sources(key._2), conf.date)

  def reduceWithFeature(facts: NonEmptyList[Fact], dictionary: ConcreteGroup, date: Date): List[Fact] =
    ModeHandler.get(dictionary.definition.mode).reduce(facts, dictionary.range(date).fromOrMax)
}
