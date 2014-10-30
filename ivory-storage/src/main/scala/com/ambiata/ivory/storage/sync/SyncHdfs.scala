package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.distcopy._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3._
import org.apache.hadoop.fs._

import scalaz.{Name =>_,_}, Scalaz._, effect._, effect.Effect._

object SyncHdfs {

  def toLocal(sourceBase: DirPath, files: List[FilePath], destinationBase: DirPath, cluster: Cluster): ResultTIO[Unit] = {
    val fs: FileSystem = FileSystem.get(cluster.hdfsConfiguration)
    for {
      s <- Hdfs.exists(new Path(sourceBase.path)).run(cluster.hdfsConfiguration)
      _ <- ResultT.unless[IO](s, ResultT.fail(s"Source base does not exists (${sourceBase.path})"))
      // TODO replace relativeTo with removeCommonPrefix
      _ <- files.traverseU(f => {
        val outputPath = destinationBase </> f.relativeTo(sourceBase)
        Hdfs.readWith(new Path(f.path), input => {
          ResultT.using(outputPath.asAbsolute.toOutputStream) { output =>
            Streams.pipe(input, output, ChunkSize)
          }
        })
      }).run(cluster.hdfsConfiguration)
    } yield ()
  }

  def toHdfs(data:Datasets, source: ShadowRepository, destination: Repository): Repository =
    ShadowRepository.toRepository(source)


   /* === UPLOAD ===
    Validation
    - Check sourceBase exists
    */
  def toS3(sourceBase: DirPath, files: List[FilePath], destinationBase: S3Prefix, cluster: Cluster): ResultTIO[Unit] = for {
    s <- Hdfs.exists(new Path(sourceBase.path)).run(cluster.hdfsConfiguration)
    _ <- ResultT.unless[IO](s, ResultT.fail(s"Source base does not exists (${sourceBase.path})"))
    // TODO replace relativeTo with removeCommonPrefix when implemented
    m <- files.traverseU(f => {
      val ff = f.relativeTo(sourceBase)
      ResultT.ok[IO, Mapping](UploadMapping(new Path(ff.path), destinationBase | ff.path))
    })
    _ <- DistCopyJob.run(Mappings(m.toVector), cluster.conf)
  } yield ()
}
