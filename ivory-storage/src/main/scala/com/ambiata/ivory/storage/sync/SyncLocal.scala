package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs.Hdfs
import org.apache.hadoop.fs.Path
import scalaz._, Scalaz._, effect.IO, effect.Effect._


object SyncLocal {
   def toHdfs(base: DirPath, destination: DirPath, cluster: Cluster): ResultTIO[Unit] = for {
     files <- Directories.list(base).map(_.map(_.asAbsolute))
     _     <- Directories.mkdirs(cluster.root.location.path </> destination)
     _     <- files.traverseU { path =>
       ResultT.using(path.asAbsolute.toInputStream) { input =>
         Hdfs.writeWith((cluster.root </> destination </> path.relativeTo(base)).toHdfsPath, { output =>
           Streams.pipe(input, output, ChunkSize)
         }).run(cluster.hdfsConfiguration)
       }
     }
   } yield ()

 }
