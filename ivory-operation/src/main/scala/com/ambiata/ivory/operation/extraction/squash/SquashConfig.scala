package com.ambiata.ivory.operation.extraction.squash

case class SquashConfig(profileSampleRate: Int)

object SquashConfig {

  def default: SquashConfig =
    SquashConfig(1000000)

  def testing: SquashConfig =
    SquashConfig(1)
}
