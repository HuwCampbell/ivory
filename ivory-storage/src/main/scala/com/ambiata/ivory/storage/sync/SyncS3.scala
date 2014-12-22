package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core.Cluster
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.distcopy._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.{S3Address, S3Prefix}
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._, effect.IO

object SyncS3 {

  /* === DOWNLOAD ===
   Validation
   - Check that the source prefix exists
   */
  def toHdfs(sourceBase: S3Prefix, sourceFiles: List[S3Address], outputBase: DirPath, cluster: Cluster): RIO[Unit] = for {
    s <- sourceBase.exists.executeT(cluster.s3Client)
    _ <- ResultT.unless[IO](s, ResultT.fail(s"Source base does not exists (${sourceBase.render})"))
    d <- sourceFiles.traverseU(ss => ss.removeCommonPrefix(sourceBase) match {
      case Some(v) => ResultT.ok[IO, Mapping](DownloadMapping(ss, new Path((cluster.rootDirPath </> outputBase </> FilePath.unsafe(v)).path)))
      case None    => ResultT.failIO[Mapping](s"Source file (${ss.render}) does not share the commmon prefix (${sourceBase.render})")
    })
    _ <- DistCopyJob.run(Mappings(d.toVector), cluster.conf)
  } yield ()
}
