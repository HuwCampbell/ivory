package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import org.apache.hadoop.fs.Path
import scalaz._, Scalaz._, effect.Effect._

object SyncLocal {

  def toHdfs(base: DirPath, files: List[FilePath], baseOutput: DirPath, cluster: Cluster): RIO[Unit] = for {
    s <- Directories.exists(base)
    _ <- RIO.unless(s, RIO.fail(s"Source base does not exists ($base)"))
    _ <- files.traverseU(f => {
      removeCommonPath(f, base) match {
      case Some(p) => {
        val outputPath = cluster.rootDirPath </> baseOutput </> p
        RIO.using(f.asAbsolute.toInputStream) { input =>
          Hdfs.writeWith(new Path(outputPath.path), { output =>
            Streams.pipe(input, output, ChunkSize)
          }).run(cluster.hdfsConfiguration)
        }}
        case None    =>
          RIO.failIO[Unit](s"Source file ($f) does not share the common path ($base)")
      }
    })
  } yield ()
}
