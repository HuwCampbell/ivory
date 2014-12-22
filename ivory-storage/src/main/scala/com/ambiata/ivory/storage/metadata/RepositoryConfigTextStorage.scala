package com.ambiata.ivory.storage.metadata

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import org.joda.time.DateTimeZone
import scalaz._, Scalaz._, effect._

/**
 * Simple key/value store for repository configuration, stored as JSON.
 *
 * Currently only `timezone` is supported.
 *
 * {{{
 *   {
 *     "timezone": "Australia/Sydney"
 *   }
 * }}}
 */
object RepositoryConfigTextStorage {

  def latestId: RepositoryTIO[Option[RepositoryConfigId]] = RepositoryT.fromResultTIO(repository =>
    IdentifierStorage.latestId(repository, Repository.configs).map(_.map(RepositoryConfigId.apply))
  )

  def load: RepositoryTIO[RepositoryConfig] = for {
    id     <- latestId
    config <- loadById(id.getOrElse(RepositoryConfigId.initial))
  } yield config

  def loadById(id: RepositoryConfigId): RepositoryTIO[RepositoryConfig] = RepositoryT.fromResultTIO(repository => {
    val key = Repository.config(id)
    for {
      exists <- repository.store.exists(key)
      config <-
        if (exists)
          for {
            lines  <- repository.store.utf8.read(key)
            result <- ResultT.fromDisjunctionString[IO, RepositoryConfig](Parse.decodeEither[RepositoryConfig](lines))
          } yield result
        else RepositoryConfig.deprecated.point[RIO]
    } yield config
  })

  def store(config: RepositoryConfig): RepositoryTIO[RepositoryConfigId] = RepositoryT.fromResultTIO(repository => for {
    nextId <- IdentifierStorage.nextIdOrFail(repository, Repository.configs).map(RepositoryConfigId.apply)
    _      <- repository.store.utf8.write(Repository.config(nextId), config.asJson.spaces2)
  } yield nextId)

  def toJson(config: RepositoryConfig): String =
    config.asJson.spaces2

  implicit def RepositoryConfigCodecJson : CodecJson[RepositoryConfig] = CodecJson(
    config => ("timezone" := config.timezone) ->: jEmptyObject,
    c => (c --\ "timezone").as[DateTimeZone].map(RepositoryConfig.apply)
  )

  implicit def DateTimeZoneCodecJson: CodecJson[DateTimeZone] = CodecJson.derived(
    EncodeJson(_.getID.asJson),
    DecodeJson(c => c.as[String].flatMap(s => DecodeResult(DateTimeZoneUtil.forID(s).leftMap(_ -> c.history))))
  )
}
