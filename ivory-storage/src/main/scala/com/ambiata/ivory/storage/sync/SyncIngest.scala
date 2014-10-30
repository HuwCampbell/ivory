package com.ambiata.ivory.storage.sync

import java.io.File
import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.saws.s3._

import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._, effect.IO

object SyncIngest {

  /* How should this work one might ask?
     Example
     1. Given an `InputDataset(<base>/2012-04-01/facts.txt)`
       <base>
       └── 2014-04-01
          └── facts.txt
        Return a `ShadowInputDataset(<tmp>/<tmp_sync>/<uuid>/facts.txt)`
       <repo_tmp>
       └── <tmp_sync>
          └── ffc748cc-f213-40dc-9f85-fee935adeec4
             └── facts.txt
      2. Give the structure
        <base>
        └── 2014-04-03
           └── facts
              └── factset
         And the `InputDataset(<base>/2014-04-03/facts)`
         Return `ShadowInputDataset(<tmp>/<tmp_sync>/<uuid>/facts/)` with
         the following file strcutre
        <repo_tmp>
        └── <tmp_sync>
           └── ffc748cc-f213-40dc-9f85-fee935adeec4
              └── facts
                 └── factset
                                                                                         */
  def inputDataset(input: InputDataset, cluster: Cluster): ResultTIO[ShadowInputDataset] = {
    val outputPath = DirPath.unsafe(s"tmp/shadow/${UUID.randomUUID()}")
    def getOutput(opt: Option[String]): ShadowInputDataset = opt match {
      case Some(v) =>
        ShadowInputDataset(HdfsLocation((cluster.rootDirPath </> outputPath </> FilePath.unsafe(v)).path))
      case None =>
        ShadowInputDataset(HdfsLocation((cluster.rootDirPath </> outputPath.toFilePath).path))
    }

    input.location match {
      case LocalIvoryLocation(l @ LocalLocation(path)) => for {
        files <- if (l.dirPath.toFile.isFile) ResultT.ok[IO, List[FilePath]](List(l.filePath))
                 else Directories.list(l.dirPath)
        c     = l.dirPath
        pr    = DirPath(c.names.init.toVector, c.isAbsolute)
        _     <- SyncLocal.toHdfs(pr, files, outputPath, cluster)
        r     <- ResultT.ok[IO, ShadowInputDataset](getOutput(c.components.lastOption))
      } yield r

      case S3IvoryLocation(S3Location(bucket, key), _) => {
        val pattern = S3Pattern(bucket, key)
        val keys = key.split("/").toList
        val prefix = S3Prefix(bucket, keys.init.mkString("/"))
        for {
          files <- getS3Info(pattern).executeT(cluster.s3Client)
          _     <- SyncS3.toHdfs(prefix, files, outputPath, cluster)
          r     <- ResultT.ok[IO, ShadowInputDataset](getOutput(keys.lastOption))
        } yield r
      }

      case HdfsIvoryLocation(h @ HdfsLocation(_), _, _, _) =>
        ResultT.ok(ShadowInputDataset(h))
    }
  }

  def toCluster(datasets: Datasets, source: Repository, cluster: Cluster): ResultTIO[ShadowRepository] =
    (source.root.location match {
      case S3Location(bucket, key) =>
       getS3data(datasets, bucket, key, cluster) >>= (files =>
          SyncS3.toHdfs(S3Prefix(bucket, key), files, cluster.rootDirPath, cluster))
      case l @ LocalLocation(_) =>
        getLocalFilePaths(datasets, l.dirPath, cluster) >>= (files =>
          SyncLocal.toHdfs(l.dirPath, files, DirPath.Empty, cluster))
      case HdfsLocation(_) =>
        ResultT.unit[IO]
    }).as(source match {
      case HdfsRepository(HdfsIvoryLocation(h, _, _, _)) =>
        ShadowRepository.fromDistCopyConfiguration(new Path(h.path), cluster.conf)
      case _ =>
        ShadowRepository.fromDistCopyConfiguration(cluster.root, cluster.conf)
    })
}
