package com.ambiata.ivory.operation.model

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.entities.Entities

import scalaz._, Scalaz._


case class ChordModelConf(entities: Entities, dictionary: Dictionary)

object ChordModel extends MapReduceSimple[Prioritized[Fact], Fact, ChordModelConf] {
  type M = SnapshotModel.M
  type K = SnapshotModel.K
  type S = SnapshotModel.S

  def map(facts: List[Prioritized[Fact]], conf: ChordModelConf): List[(K, S, Fact)] =
    facts.filter(f => conf.entities.keep(f.value))
      .map(SnapshotModel.mapKeyed)

  def secondary: Order[S] =
    SnapshotModel.secondary

  def reduce(key: K, facts: NonEmptyList[Fact], conf: ChordModelConf): List[Fact] =
    reduceWithDates(facts, conf.dictionary.byConcrete.sources(key._2), conf.entities.entities.get(key._1).toList.map(Date.unsafeFromInt))

  def reduceWithDates(facts: NonEmptyList[Fact], dictionary: ConcreteGroup, dates: List[Date]): List[Fact] = {
    (Date.minValue :: dates.reverse).sliding(2).toList.flatMap {
      case List(d1, d2) =>
        facts.toList.filter(f => d1 < f.date && f.date <= d2) match {
          case h :: t =>
            // This isn't really ideal - we're doing too much work here
            // We should really have a better way of filtering facts
            SnapshotModel.reduceWithFeature(NonEmptyList(h, t: _*), dictionary, d2)
          case Nil =>
            Nil
        }
    }
  }
}
