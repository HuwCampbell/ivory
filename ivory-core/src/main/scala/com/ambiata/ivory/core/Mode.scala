package com.ambiata.ivory.core

sealed trait Mode {
  import Mode._

  def fold[X](
    state: => X
  , set: => X
  , keyedSet: String => X
  ): X = this match {
    case State => state
    case Set => set
    case KeyedSet(key) => keyedSet(key)
  }

  def render: String =
    fold("state", "set", "keyed_set," + _)
}

object Mode {
  case object State extends Mode
  case object Set extends Mode
  case class KeyedSet(key: String) extends Mode

  def state: Mode =
    State

  def set: Mode =
    Set

  def keyedSet(key: String): Mode =
    KeyedSet(key)

  def fromString(s: String): Option[Mode] = s.split(",", 2).toList match {
    case List("state") =>
      Some(State)
    case List("set") =>
      Some(Set)
    case List("keyed_set", key) =>
      Some(KeyedSet(key))
    case _ =>
      None
  }
}
