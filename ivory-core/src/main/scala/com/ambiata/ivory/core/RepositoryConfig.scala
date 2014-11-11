package com.ambiata.ivory.core

import org.joda.time.DateTimeZone

import scalaz._

/** Represents the repository-wide configuration */
case class RepositoryConfig(timezone: DateTimeZone)

object RepositoryConfig {

  implicit def FeatureIdEqual: Equal[RepositoryConfig] =
    Equal.equalA[RepositoryConfig]

  /** For repositories that were created before configuration was mandatory */
  def deprecated: RepositoryConfig =
    RepositoryConfig(DateTimeZone.forID("Australia/Sydney"))

  /** For testing only! */
  def testing: RepositoryConfig =
    RepositoryConfig(DateTimeZone.forID("Australia/Sydney"))
}
