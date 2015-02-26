package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.FlagLookup
import com.ambiata.ivory.storage.lookup.FeatureLookups

import scala.collection.JavaConverters._

/** Handle filtering of facts for different modes in snapshot/chord */
trait ModeReducer {
  /** My kingdom for decent rank-2 types */
  type X
  def seed: X
  def step(state: X, fact: Fact): X
  def accept(state: X, fact: Fact): Boolean
}

object ModeReducer {

  def fromMode(mode: Mode): ModeReducer =
    mode.fold(ModeReducerState, ModeReducerSet, _ => NotImplemented.keyedSet)

  def fromLookup(flag: FlagLookup): Array[ModeReducer] =
    FeatureLookups.sparseMapToArray(flag.flags.asScala.map(x => x._1.intValue -> x._2.booleanValue).mapValues {
      case true =>
        ModeReducerSet
      case false =>
        ModeReducerState
    }.toList, ModeReducerState)
}

object ModeReducerState extends ModeReducer {

  type X = DateTime

  def seed: DateTime =
    DateTime.unsafeFromLong(-1)

  def step(datetime: DateTime, f: Fact): DateTime =
    f.datetime

  def accept(datetime: DateTime, f: Fact): Boolean =
    datetime != f.datetime
}

object ModeReducerSet extends ModeReducer {

  type X = Unit

  def seed: Unit =
    ()

  def step(state: Unit, f: Fact): Unit =
    state

  def accept(state: Unit, f: Fact): Boolean =
    true
}
