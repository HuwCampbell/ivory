package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.mundane.control.RIO
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.core.S3Action
import com.ambiata.saws.s3._
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._

object Sync {
  val ChunkSize = 1024000

  def removeCommonPath(input: FilePath, remove: DirPath): Option[FilePath] = {
    def ll(x: List[String], y: List[String]): Option[String] = {
      val p = y.zipL(x)
      if (p.forall( { case (l,r) => Some(l) == r }))
        Some(x.drop(y.size).mkString("/"))
      else
        None
    }
    ll(input.names.map(_.name), remove.components).map(FilePath.unsafe(_))
  }

  def getKeys(data: Datasets): List[Key] = data.sets.flatMap(_.value match {
    case FactsetDataset(Factset(fid, parts)) =>
      parts.map(Repository.factset(fid) / _.key)
    case SnapshotDataset(Snapshot(sid, _, _)) =>
      (Repository.snapshots / sid.asKeyName) :: Nil
  })

  def getS3data(data: Datasets, bucket: String, key: String, cluster: Cluster): RIO[List[S3Address]] = data.sets.traverseU(_.value match {
    case FactsetDataset(Factset(fid, parts)) =>
      parts.map(Repository.factset(fid) / _.key).traverseU(x =>
        getS3Info(S3Pattern(bucket, (Key.unsafe(key) / x).name)).executeT(cluster.s3Client)).map(_.flatten)
    case SnapshotDataset(Snapshot(sid, _, _)) =>
      getS3Info(S3Pattern(bucket, (Repository.snapshots / sid.asKeyName).name)).executeT(cluster.s3Client)
  }).map(_.flatten)

  def getHdfsFilePaths(data: Datasets, root: DirPath, cluster: Cluster): RIO[List[FilePath]] = data.sets.traverseU(_.value match {
    case FactsetDataset(Factset(fid, parts)) =>
      parts.map(Repository.factset(fid) / _.key).traverseU(x =>
        getFilePathInfosOnHdfs(root </> DirPath.unsafe(x.name)).run(cluster.hdfsConfiguration)).map(_.flatten)
    case SnapshotDataset(Snapshot(sid, _, _)) =>
      getFilePathInfosOnHdfs(root </> DirPath.unsafe((Repository.snapshots / sid.asKeyName).name)).run(cluster.hdfsConfiguration)
  }).map(_.flatten)

  def getLocalFilePaths(data: Datasets, root: DirPath, cluster: Cluster): RIO[List[FilePath]] = data.sets.traverseU(_.value match {
    case FactsetDataset(Factset(fid, parts)) =>
      parts.map(Repository.factset(fid) / _.key).traverseU(x =>
        Directories.list(root </> DirPath.unsafe(x.name))).map(_.flatten)
    case SnapshotDataset(Snapshot(sid, _, _)) =>
      Directories.list(root </> DirPath.unsafe((Repository.snapshots / sid.asKeyName).name))
  }).map(_.flatten)

  def getS3Info(pattern: S3Pattern): S3Action[List[S3Address]] =
    pattern.listS3.map(_.map(_.s3))

  def getFilePathInfosOnHdfs(path: DirPath): Hdfs[List[FilePath]] = for {
    fs    <- Hdfs.filesystem
    paths <- Hdfs.globFilesRecursively(new Path(path.path))
    infos = paths.flatMap { p =>
      Location.fromUri(p.toUri.toString).toOption.flatMap {
        location => location match {
          case l@LocalLocation(_) => l.filePath.some
          case h@HdfsLocation(_)  => h.filePath.some
          case S3Location(_, _)   => none
        }
      }
    }
  } yield infos

}
