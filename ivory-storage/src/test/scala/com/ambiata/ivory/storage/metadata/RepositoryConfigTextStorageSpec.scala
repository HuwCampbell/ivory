package com.ambiata.ivory.storage.metadata

import argonaut._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.metadata.RepositoryConfigTextStorage._
import com.ambiata.ivory.storage.repository.TemporaryRepositoriesT
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core.TemporaryType
import org.joda.time.DateTimeZone
import org.specs2._

import scalaz.scalacheck.ScalazProperties

class RepositoryConfigTextStorageSpec extends Specification with ScalaCheck { def is = s2"""

  Store and load a configuration into a repository     $storeLoad       ${tag("store")}
  Load configuration from an empty repository          $empty           ${tag("store")}

  Equal laws                                           ${ScalazProperties.equal.laws[RepositoryConfig]}
  Encode and decode as JSON                            ${ArgonautProperties.encodedecode[RepositoryConfig]}
  Try to load invalid timezone ID                      $invalidTimezone
                                                       """

  def storeLoad = prop((config: RepositoryConfig) =>
    TemporaryRepositoriesT.withRepositoryT(TemporaryType.Posix) {
      for {
        _         <- store(config)
        newConfig <- load
      } yield newConfig
    } must beOkValue(config)
  ).set(minTestsOk = 10)

  def empty =
    TemporaryRepositoriesT.withRepositoryT(TemporaryType.Posix)(load) must beOkValue(RepositoryConfig.deprecated)

  def invalidTimezone =
    Parse.decodeEither[DateTimeZone]("Australia/XXX").toEither must beLeft
}
