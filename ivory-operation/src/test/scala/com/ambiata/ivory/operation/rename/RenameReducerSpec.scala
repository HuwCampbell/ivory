package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2.{ScalaCheck, Specification}

class RenameReducerSpec extends Specification with ScalaCheck { def is = s2"""

  Rename state                                                $renameState
  Rename set                                                  $renameSet
"""

  def renameState = prop((fs: List[Fact]) => {
    val facts = new RenameFacts(fs)
    facts.reduce(isSet = false) ==== facts.factGroups.map(_._2.head)
  })

  def renameSet = prop((fs: List[Fact]) => {
    val facts = new RenameFacts(fs)
    facts.reduce(isSet = true) ==== facts.facts
  })

  class RenameFacts(fs: List[Fact]) {
    // The date is irrelevant to rename - it's expected to be grouped by date already
    lazy val factGroups: List[((String, Time), List[Fact])] =
      fs.groupBy(f => f.entity -> f.time).toList.sortBy(f => f._1._1 -> f._1._2.seconds)
    lazy val facts = factGroups.flatMap(_._2)

    def reduce(isSet: Boolean): List[Fact] = {
      val state = RenameReducerState(isSet)
      facts.filter { fact =>
        val tfact = fact.toThrift
        if (state.accept(tfact)) {
          state.update(tfact)
          true
        } else false
      }
    }
  }
}
