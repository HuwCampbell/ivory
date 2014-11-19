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

  def toLocal(base: DirPath, files: List[FilePath], baseOutput: DirPath, cluster: Cluster): ResultTIO[Unit] = {
    val fs: FileSystem = FileSystem.get(cluster.hdfsConfiguration)

    for {
      s <- Hdfs.exists(new Path(base.path)).run(cluster.hdfsConfiguration)
      _ <- ResultT.unless[IO](s, ResultT.fail(s"Source base does not exists (${base.path})"))
      _ <- files.traverseU(f =>
        removeCommonPath(f, base) match {
          case Some(p) =>
            val outputPath = baseOutput </> p
            Directories.mkdirs(DirPath(outputPath.names.init.toVector, outputPath.isAbsolute)) >>
            Hdfs.readWith(new Path(f.path), input => {
              ResultT.using(outputPath.asAbsolute.toOutputStream) { output =>
                Streams.pipe(input, output, ChunkSize)
              }
            }).run(cluster.hdfsConfiguration)
          case None =>
            ResultT.failIO[Unit](s"Source file ($f) does not share the common path (base)")
        }
      )
    } yield ()
  }

  def toHdfs(data:Datasets, source: ShadowRepository, destination: Repository): Repository =
    ShadowRepository.toRepository(source)


   /* === UPLOAD ===
    Validation
    - Check sourceBase exists
    */
  def toS3(base: DirPath, files: List[FilePath], baseOutput: S3Prefix, cluster: Cluster): ResultTIO[Unit] = for {
    s <- Hdfs.exists(new Path(base.path)).run(cluster.hdfsConfiguration)
    _ <- ResultT.unless[IO](s, ResultT.fail(s"Source base does not exists (${base.path})"))
    m <- files.traverseU(f => {
      removeCommonPath(f, base) match {
        case Some(p) =>
          ResultT.ok[IO, Mapping](UploadMapping(new Path(f.path), baseOutput | p.path))
        case None =>
          ResultT.failIO[Mapping](s"Source file ($f) does not share the common path (base)")
      }
    })
    _ <- DistCopyJob.run(Mappings(m.toVector), cluster.conf)
  } yield ()
}
