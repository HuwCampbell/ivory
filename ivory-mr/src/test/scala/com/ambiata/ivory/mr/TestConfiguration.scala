package com.ambiata.ivory.mr

import com.nicta.scoobi.Scoobi.ScoobiConfiguration

trait TestConfigurations {
  def scoobiConfiguration: ScoobiConfiguration = {
    val sc = ScoobiConfiguration()
    sc.set("hadoop.tmp.dir", "/tmp/" + java.util.UUID.randomUUID.toString)
    sc
  }
}

object TestConfigurations extends TestConfigurations
