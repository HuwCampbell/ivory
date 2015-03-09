package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.FlagLookup
import com.ambiata.ivory.mr.MutableOption
import com.ambiata.ivory.storage.lookup.FeatureLookups

import scala.collection.JavaConverters._

/** Handle filtering of facts for different modes in snapshot/chord */
trait ModeReducer {
  /** My kingdom for decent rank-2 types */
  type X
  def seed: X
  /** Return the next state if the step is valid, or none otherwise */
  def step(state: X, option: MutableOption[X], fact: Fact): MutableOption[X]
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

  def step(datetime: DateTime, option: MutableOption[DateTime], f: Fact): MutableOption[DateTime] =
    if (datetime != f.datetime) MutableOption.set(option, f.datetime)
    else MutableOption.setNone(option)
}

object ModeReducerSet extends ModeReducer {

  type X = Unit

  def seed: Unit =
    ()

  def step(state: Unit, option: MutableOption[Unit], f: Fact): MutableOption[Unit] =
    MutableOption.set(option, state)
}
