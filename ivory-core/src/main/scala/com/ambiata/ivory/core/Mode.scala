package com.ambiata.ivory.core

sealed trait Mode {
  import Mode._

  def fold[X](
    state: => X
  , set: => X
  ): X = this match {
    case State => state
    case Set => set
  }

  def render: String =
    fold("state", "set")

  def isState: Boolean =
    fold(true, false)

  def isSet: Boolean =
    fold(false, true)
}

object Mode {
  case object State extends Mode
  case object Set extends Mode

  def state: Mode =
    State

  def set: Mode =
    Set

  def fromString(s: String): Option[Mode] = s match {
    case "state" =>
      Some(State)
    case "set" =>
      Some(Set)
    case _ =>
      None
  }
}
