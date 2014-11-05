package com.ambiata.ivory.operation.statistics

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr._
import FactFormats._
import com.ambiata.poacher.scoobi._

object FactCount {

  def flatFacts(path: Path): ScoobiAction[Long] = ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
    val facts = valueFromSequenceFile[Fact](path.toString)
    facts.size.run
  })

}
