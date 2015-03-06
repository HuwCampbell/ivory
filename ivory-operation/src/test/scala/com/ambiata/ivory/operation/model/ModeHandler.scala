package com.ambiata.ivory.operation.model

import com.ambiata.ivory.core._

import scalaz._

trait ModeHandler {

  def reduce(facts: NonEmptyList[Fact], window: Date): List[Fact]
}

object ModeHandler {

  def get(mode: Mode): ModeHandler =
    mode.fold(ModeHandlerState, ModeHandlerSet, _ => NotImplemented.keyedSet)
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
