package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.ivory.storage.plan._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.fs.{Path, Hdfs}

import scalaz._, Scalaz._, effect.IO

object SyncExtract {

   def outputDataSet(input: ShadowOutputDataset, cluster: Cluster, output: OutputDataset): ResultTIO[Unit] =
     (input.location.location, output.location.location) match {
       case (HdfsLocation(p1), LocalLocation(p2)) => SyncHdfs.toLocal(p1, cluster, p2)
       case (_, _)                                => unImplementedSyncOperation
     }

   def toRepository(data:Datasets, cluster: Cluster, repo: Repository): ResultTIO[Unit] = repo match {
     case LocalRepository(LocalIvoryLocation(LocalLocation(path))) => getKeys(data).traverseU(key => SyncHdfs.toLocal(cluster.root.location.path </> FileName.unsafe(key.name), cluster, path)).void
     case _                                                           => unImplementedSyncOperation
   }

 }
