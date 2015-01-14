package com.ambiata.ivory.operation.update

import argonaut._, Argonaut._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.manifest._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.partition._
import com.ambiata.ivory.storage._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._

object UpdateV0 {
  def update: RepositoryTIO[Unit] = for {
    _ <- updateFactsets
    _ <- updateSnapshots
  } yield ()

  def updateFactsets: RepositoryTIO[Unit] = RepositoryT.fromRIO(repository =>
    Metadata.findFactsets(repository).flatMap(updateFactsetsIds(repository, _))
  )

  def updateFactsetsIds(repository: Repository, factsets: List[FactsetId]): RIO[Unit] = for {
    _ <- factsets.traverseU(f => for {
      partitions <- Partitions.scrapeFromFactset(repository, f)
      _    <- FactsetManifest.io(repository, f).write(FactsetManifest.create(f, FactsetFormat.V2, partitions))
    } yield ())
  } yield()

  def updateSnapshots: RepositoryTIO[Unit] = RepositoryT.fromRIO(repository => for {
    snapshots <- repository.store.listHeads(Repository.snapshots).map(_.filterHidden)
    sm <- snapshots.traverseU { k =>
      SnapshotId.parse(k.name).cata(s =>
        SnapshotMetadataV1.read(repository, s)
          .flatMap(_.cata(_.some.pure[RIO], SnapshotMetadataV0.read(repository, s)))
          .flatMap(_.cata(_.some.pure[RIO], RIO.putStrLn(s"WARNING: Snapshot skipped ${k.name}") >> RIO.ok(none[SnapshotManifest])))
      , RIO.putStrLn(s"WARNING: Invalid snapshot path ${k.name}") >> RIO.ok(none[SnapshotManifest]))
      }
    _ <- sm.flatten.traverseU(s => SnapshotManifest.io(repository, s.snapshot).write(s))
  } yield ())

  case class SnapshotMetadataV0(snapshotId: SnapshotId, date: Date, storeOrCommit: FeatureStoreId \/ CommitId)

  object SnapshotMetadataV0 {

    import com.ambiata.mundane.parse.ListParser

    val key = KeyName.unsafe(".snapmeta")

    def read(repository: Repository, snapshotId: SnapshotId): RIO[Option[SnapshotManifest]] = {
      val path = Repository.snapshot(snapshotId) / key
      for {
        exists <- repository.store.exists(path)
        sm     <- if (exists) for {
          lines       <- repository.store.linesUtf8.read(path)
          // Ensure we have at least 3 lines (to include new commitId)
          safeLines    = if (lines.length == 2) lines ++ List("") else lines
          id           = path.components.lastOption.flatMap(s => SnapshotId.parse(s.name)).getOrElse(SnapshotId.initial)
          md          <- RIO.fromDisjunctionString[SnapshotMetadataV0](parser(id).run(safeLines).disjunction)
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

    def read(repository: Repository, snapshotId: SnapshotId): RIO[Option[SnapshotManifest]] = for {
      path   <- RIO.ok(Repository.snapshot(snapshotId) / key)
      exists <- repository.store.exists(path)
      sm <- if (exists) for {
        json <- repository.store.utf8.read(path)
        md   <- RIO.fromDisjunctionString[SnapshotMetadataV1](json.decodeEither[SnapshotMetadataV1])
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
