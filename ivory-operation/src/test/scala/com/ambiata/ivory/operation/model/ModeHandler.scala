package com.ambiata.ivory.operation.model

import com.ambiata.ivory.core._

import scalaz._

trait ModeHandler {

  def reduce(facts: NonEmptyList[Fact], window: Date): List[Fact]
}

object ModeHandler {

  def get(mode: Mode): ModeHandler =
    mode.fold(ModeHandlerState, ModeHandlerSet, keys => new ModeHandlerKeyedSet(keys))
}

object ModeHandlerState extends ModeHandler {

  def reduce(facts: NonEmptyList[Fact], window: Date): List[Fact] = {
    val noPriorityFacts = facts.list.foldLeft(List[Fact]()) {
      case (Nil, f) =>
        List(f)
      case (l@(h :: t), f) =>
        if (h.datetime != f.datetime) f :: l else l
    }.reverse
    val (oldFacts, newFacts) = noPriorityFacts.partition(!Window.isFactWithinWindow(window, _))
    oldFacts.lastOption.toList ++ newFacts
  }
}

object ModeHandlerSet extends ModeHandler {

  def reduce(facts: NonEmptyList[Fact], window: Date): List[Fact] = {
    val (oldFacts, newFacts) = facts.list
      .partition(!Window.isFactWithinWindow(window, _))
    oldFacts.lastOption.toList ++ newFacts
  }
}

class ModeHandlerKeyedSet(keys: List[String]) extends ModeHandler {

  def reduce(facts: NonEmptyList[Fact], window: Date): List[Fact] = {
    val noPriorityFacts = facts.list.foldLeft(List[Fact]()) {
      case (Nil, f) =>
        List(f)
      case (l@(h :: t), f) =>
        (h.value, f.value) match {
          case (StructValue(sv1), StructValue(sv2)) =>
            // Just use the natural equality of PrimitiveValue - works well enough and should match the byte comparison
            if (h.datetime != f.datetime || keys.exists(key => sv1.get(key) != sv2.get(key))) f :: l else l
          case (_, _) =>
            sys.error(s"Incorrectly generated fact for keyed_set: ${h.value}")
        }
    }.reverse
    val (oldFacts, newFacts) = noPriorityFacts.partition(!Window.isFactWithinWindow(window, _))
    oldFacts.lastOption.toList ++ newFacts
  }
}
