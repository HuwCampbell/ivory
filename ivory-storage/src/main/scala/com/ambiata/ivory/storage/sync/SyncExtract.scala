package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.notion.distcopy._
import com.ambiata.poacher.hdfs._
import com.ambiata.saws.s3._

import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._, effect.IO

object SyncExtract {

  /*
   File example:
     `ShadowOutputDataset("/a/b") -> OutputDatset ("/tmp/foo")`
     where the contents of b and foo are equal

   Directory example:
     Given the directory:
       /
       └── a
          └── b
             └── c
       `ShadowOutputDataset("/a/b") -> OutputDatset ("/tmp/foo")`
     outputs the directory structure
       /
       └── tmp
          └── foo
             └── c
                                                                             */
  def outputDataset(input: ShadowOutputDataset, cluster: Cluster, output: OutputDataset): ResultTIO[Unit] = {
    val path: Path = new Path(input.location.path)
    val dir = DirPath.unsafe(input.location.path)
    for {
      _ <- Hdfs.mustExistWithMessage(path, s"ShadowOuputDataset ( ${input.location} ) does not exist.").run(cluster.hdfsConfiguration)
      _ <- output.location match {
        case l @ LocalLocation(_) => for {
          // Poacher is ok and handles both files and directories
          f <- Hdfs.globFilesRecursively(path).run(cluster.hdfsConfiguration).map(_.map(z => FilePath.unsafe(z.toString)))
          _ <- SyncHdfs.toLocal(DirPath.unsafe(input.location.path), f, l.dirPath, cluster)
        } yield ()

        case S3Location(bucket, key) => for {
          d <- Hdfs.isDirectory(path).run(cluster.hdfsConfiguration)
          _ <- d match {
            case true =>
              for {
                f <- Hdfs.globFilesRecursively(path).run(cluster.hdfsConfiguration).map(_.map(z => FilePath.unsafe(z.toString)))
                _ <- SyncHdfs.toS3(dir, f, S3Prefix(bucket, key), cluster)
              } yield ()
            case false =>
              DistCopyJob.run(Mappings(Vector(UploadMapping(path, S3Address(bucket, key)))), cluster.conf)
          }
        } yield ()

        case HdfsLocation(p) =>
          val pp = new Path(p)
          if (path == pp) ResultT.unit[IO]
          else (for {
            _ <- Hdfs.mustNotExistWithMessage(pp, s"Target directory ( $pp ) already exists.")
            _ <- Hdfs.mv(path, pp).void
          } yield ()).run(cluster.hdfsConfiguration)
      }
    } yield ()
  }

  def toRepository(data: Datasets, shadow: ShadowRepository, cluster: Cluster, repo: Repository): ResultTIO[Unit] =
    unImplementedSyncOperation

}
