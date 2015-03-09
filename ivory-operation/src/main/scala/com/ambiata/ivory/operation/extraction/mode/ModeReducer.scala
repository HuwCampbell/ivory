package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.MutableOption
import com.ambiata.ivory.storage.lookup.FeatureLookups
import com.ambiata.mundane.bytes.Buffer

import org.apache.hadoop.io.WritableComparator

import scalaz.{State => _, _}, Scalaz._


/** Handle filtering of facts for different modes in snapshot/chord */
trait ModeReducer {
  /** My kingdom for decent rank-2 types */
  type X
  def seed: X
  /** Return the next state if the step is valid, or none otherwise */
  def step(state: X, option: MutableOption[X], fact: Fact): MutableOption[X]
}

object ModeReducer {

  def fromMode(mode: Mode, encoding: Encoding): String \/ ModeReducer =
    mode.fold(
      ModeReducerState.right,
      ModeReducerSet.right,
      key => ModeKey.fromEncoding(key, encoding)
        .map(modeKey => new ModeReducerKeyedSet(modeKey))
    )

  def fromDictionary(dictionary: Dictionary): Array[ModeReducer] =
    fromLookup(ModeKey.toModeAndEncoding(dictionary))

  def fromLookup(modes: Map[Int, (Mode, Encoding)]): Array[ModeReducer] =
    FeatureLookups.sparseMapToArray(modes.traverseU((fromMode _).tupled)
      .fold(Crash.error(Crash.Invariant, _), identity).toList, ModeReducerState)
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

class ModeReducerKeyedSet(key: ModeKey) extends ModeReducer {

  import ModeReducerKeyedSet.State

  type X = State

  def seed: State =
    State(Buffer.empty(255), Buffer.empty(255), DateTime.unsafeFromLong(-1))

  def step(state: State, option: MutableOption[State], f: Fact): MutableOption[State] = {
    Buffer.reset(state.buffer2)
    state.buffer2 = key.append(f, state.buffer2)
    if (state.datetime == f.datetime) {
      if (WritableComparator.compareBytes(
        state.buffer1.bytes, state.buffer1.offset, state.buffer1.length,
        state.buffer2.bytes, state.buffer2.offset, state.buffer2.length
      ) != 0) {
        MutableOption.set(option, State.swapBuffers(state))
      } else
        MutableOption.setNone(option)
    } else {
      state.datetime = f.datetime
      MutableOption.set(option, State.swapBuffers(state))
    }
  }
}

object ModeReducerKeyedSet {

  case class State(var buffer1: Buffer, var buffer2: Buffer, var datetime: DateTime)

  object State {

    def swapBuffers(state: State): State = {
      val bufferTmp = state.buffer1
      state.buffer1 = state.buffer2
      state.buffer2 = bufferTmp
      state
    }
  }
}

