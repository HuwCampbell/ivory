package com.ambiata.ivory.core

sealed trait Mode {
  import Mode._

  def fold[X](
    state: => X
  , set: => X
  , keyedSet: List[String] => X
  ): X = this match {
    case State => state
    case Set => set
    case KeyedSet(key) => keyedSet(key)
  }

  def render: String =
    fold("state", "set", "keyed_set," + _.mkString(","))
}

object Mode {
  case object State extends Mode
  case object Set extends Mode
  case class KeyedSet(keys: List[String]) extends Mode

  def state: Mode =
    State

  def set: Mode =
    Set

  def keyedSet(keys: List[String]): Mode =
    KeyedSet(keys)

  def fromString(s: String): Option[Mode] = s.split(",", -1).toList match {
    case List("state") =>
      Some(State)
    case List("set") =>
      Some(Set)
    case "keyed_set" :: keys =>
      Some(KeyedSet(keys))
    case _ =>
      None
  }
}
