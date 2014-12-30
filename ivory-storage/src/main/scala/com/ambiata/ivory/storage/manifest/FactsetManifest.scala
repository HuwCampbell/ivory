package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import scalaz._

case class FactsetManifest(version: VersionManifest, id: FactsetId, format: FactsetFormat, partitions: List[Sized[Partition]])

object FactsetManifest {
  def create(id: FactsetId, format: FactsetFormat, partitions: List[Sized[Partition]]): FactsetManifest =
    FactsetManifest(VersionManifest.current, id, format, partitions)

  def io(repository: Repository, id: FactsetId): ManifestIO[FactsetManifest] =
    ManifestIO(repository.toIvoryLocation(Repository.factset(id)))

  implicit def FactsetManifestEqual: Equal[FactsetManifest] =
    Equal.equalA[FactsetManifest]

  implicit def FactsetManifestCodecJson: CodecJson[FactsetManifest] =
    CodecManifest("factset", v => v.version -> Json(
      "id" := v.id
    , "format" := v.format
    , "partitions" := v.partitions
    ), (v, m) => for {
      id <- m.get[FactsetId]("id")
      format <- m.get[FactsetFormat]("format")
      partitions <- m.get[List[Sized[Partition]]]("partitions")
    } yield FactsetManifest(v, id, format, partitions))
}
