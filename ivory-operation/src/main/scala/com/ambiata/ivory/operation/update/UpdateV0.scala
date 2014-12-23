package com.ambiata.ivory.operation.update

import argonaut._, Argonaut._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.manifest._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.fact._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._, effect._

object UpdateV0 {

  def update: RepositoryTIO[Unit] = for {
    _ <- updateStores
    _ <- updateSnapshots
  } yield ()

  def updateStores: RepositoryTIO[Unit] = RepositoryT.fromRIO(repository => for {
    store <- Metadata.latestFeatureStoreOrFail(repository)
    _ <- store.factsets.traverseU(f => for {
      glob <- FactsetGlob.select(repository, f.value.id)
      _    <- glob.traverseU(g => FactsetManifest.io(repository, f.value.id).write(FactsetManifest.create(f.value.id, FactsetFormat.V2, g.partitions)))
    } yield ())
  } yield ())

  def updateSnapshots: RepositoryTIO[Unit] = RepositoryT.fromRIO(repository => for {
    snapshots <- repository.store.listHeads(Repository.snapshots)
    sm <- snapshots.traverseU(s =>
      SnapshotMetadataV1.read(repository, s)
        .flatMap(_.cata(_.some.pure[RIO], SnapshotMetadataV0.read(repository, s)))
    )
    _ <- sm.flatten.traverseU(s => SnapshotManifest.io(repository, s.snapshot).write(s))
  } yield ())

  case class SnapshotMetadataV0(snapshotId: SnapshotId, date: Date, storeOrCommit: FeatureStoreId \/ CommitId)

  object SnapshotMetadataV0 {

    import com.ambiata.mundane.parse.ListParser

    val key = KeyName.unsafe(".snapmeta")

    def read(repository: Repository, path: Key): RIO[Option[SnapshotManifest]] = {
      for {
        exists <- repository.store.exists(path)
        sm     <- if (exists) for {
          lines       <- repository.store.linesUtf8.read(path)
          // Ensure we have at least 3 lines (to include new commitId)
          safeLines    = if (lines.length == 2) lines ++ List("") else lines
          id           = path.components.lastOption.flatMap(s => SnapshotId.parse(s.name)).getOrElse(SnapshotId.initial)
          md          <- ResultT.fromDisjunctionString[IO, SnapshotMetadataV0](parser(id).run(safeLines).disjunction)
        } yield some(SnapshotManifest.createDeprecated(md.storeOrCommit, md.snapshotId, SnapshotFormat.V1, md.date))
        else none.point[RIO]
      } yield sm
    }

    def parser(snapshotId: SnapshotId): ListParser[SnapshotMetadataV0] = {
      import ListParser._
      for {
        date     <- localDate
        storeId  <- FeatureStoreId.listParser
        commitId <- string.map(CommitId.parse)
      } yield SnapshotMetadataV0(snapshotId, Date.fromLocalDate(date), commitId.toRightDisjunction(storeId))
    }
  }

  case class SnapshotMetadataV1(snapshotId: SnapshotId, date: Date, commitId: CommitId)

  object SnapshotMetadataV1 {

    val key = Key(KeyName.unsafe(".metadata.json"))

    def read(repository: Repository, s: Key): RIO[Option[SnapshotManifest]] = for {
      exists <- repository.store.exists(s / SnapshotMetadataV1.key)
      sm <- if (exists) for {
        json <- repository.store.utf8.read(s / SnapshotMetadataV1.key)
        md   <- ResultT.fromDisjunctionString[IO, SnapshotMetadataV1](json.decodeEither[SnapshotMetadataV1])
      } yield SnapshotManifest.create(md.commitId, md.snapshotId, SnapshotFormat.V1, md.date).some
      else none.pure[RIO]
    } yield sm

    implicit def SnapshotMetadataV1DecodeJson : DecodeJson[SnapshotMetadataV1] = DecodeJson(
      (c: HCursor) => for {
        id <- (c --\ "id").as[SnapshotId]
        date <- (c --\ "date").as[Date]
        commitId <- (c --\ "commit_id").as[CommitId]
      } yield SnapshotMetadataV1(id, date, commitId))
  }
}
