package com.ambiata.ivory.operation.extraction.mode

import com.ambiata.ivory.core._
import scalaz._, Scalaz._

object ModeModel {

  /** Given a list of facts, return the expected output */
  def snapshot(mode: Mode, facts: List[Fact]): List[Fact] =
    mode.fold(
      facts.groupBy1(f => f.entity -> f.datetime).values.map(_.head).toList,
      facts,
      keys => facts.groupBy1(f => f.value match {
        case StructValue(value) =>
          keys.flatMap(value.get)
        case _ =>
          sys.error(s"Invalid fact $f for mode $mode")
      }).values.map(_.head).toList
    )
}
