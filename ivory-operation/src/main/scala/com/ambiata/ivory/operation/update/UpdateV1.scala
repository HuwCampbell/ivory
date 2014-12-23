package com.ambiata.ivory.operation.update

import argonaut._, Argonaut._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.manifest._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._, effect.IO


object UpdateV1 {
  /* Taking V1 to V2, which means rewriting the manifest with size information for each partition. */
  def update: RepositoryTIO[Unit] = RepositoryT.fromRIO(repository => for {
    factsets <- Metadata.findFactsets(repository)
    _ <- factsets.traverseU(f => {
      val v1 = ManifestIO[FactsetManifestV1](repository.toIvoryLocation(Repository.factset(f)))
      val latest = FactsetManifest.io(repository, f)
      v1.peekOrFail.flatMap(v => v.format match {
        case MetadataVersion.V1 => for {
            m1 <- v1.readOrFail
            sized <- sized(repository, f, m1.partitions)
            m2 = FactsetManifest(VersionManifest.current, m1.id, m1.format, sized)
            _ <- latest.write(m2)
          } yield ()
        case _ =>
          ResultT.unit[IO]
      })
    })
  } yield ())

  def sized(repository: Repository, factset: FactsetId, partitions: List[Partition]): RIO[List[Sized[Partition]]] =
    partitions.traverseU(p => IvoryLocation.size(repository.toIvoryLocation(Repository.factset(factset) / p.key)).map(Sized(p, _)))

  /* This is the structure of V1 FactsetManifests. */
  case class FactsetManifestV1(version: VersionManifest, id: FactsetId, format: FactsetFormat, partitions: List[Partition])

  implicit def FactsetManifestV1CodecJson: CodecJson[FactsetManifestV1] =
    CodecManifest("factset", v => v.version -> Json(
      "id" := v.id
    , "format" := v.format
    , "partitions" := v.partitions
    ), (v, m) => for {
      id <- m.get[FactsetId]("id")
      format <- m.get[FactsetFormat]("format")
      partitions <- m.get[List[Partition]]("partitions")
    } yield FactsetManifestV1(v, id, format, partitions))
}
