package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.ivory.storage.plan._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import org.apache.hadoop.fs._

import scalaz.{Name =>_,_}, Scalaz._
import scalaz.effect.Effect._

object SyncHdfs {

   def toLocal(absoluteBasePath: DirPath, cluster: Cluster, destination: DirPath): ResultTIO[Unit] = {
     val fs: FileSystem = FileSystem.get(cluster.hdfsConfiguration)
     for {

       files <- Hdfs.globFilesRecursively(new Path(absoluteBasePath.path)).map(_.map(fs.makeQualified)).run(cluster.hdfsConfiguration)
       _     <- files.traverseU( path =>
         Hdfs.readWith(path, input => {
           val p = destination </> FilePath.unsafe(path.toUri.getPath).relativeTo(cluster.root.location.dirPath)
           Directories.mkdirs(p.dirname) >> ResultT.using(p.asAbsolute.toOutputStream) { output =>
             Streams.pipe(input, output, ChunkSize)
           }}
         ).run(cluster.hdfsConfiguration)
       )
     } yield ()
   }

   def toHdfs(data:Datasets, source: ShadowRepository, destination: Repository): Repository =
     ShadowRepository.toRepository(source)

   def toS3(data:Datasets, source: ShadowRepository, destination: Repository): Repository =
     unImplementedSyncOperation
}
