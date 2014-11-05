package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._

import org.apache.hadoop.fs.{Path, Hdfs}

import scalaz._, Scalaz._, effect.IO

object SyncExtract {

   def outputDataSet(input: ShadowOutputDataset, cluster: Cluster, output: OutputDataset): ResultTIO[Unit] =
     (input.location.location, output.location.location) match {
       case (l1 @ HdfsLocation(p1), l2 @ LocalLocation(p2)) => SyncHdfs.toLocal(l1.dirPath, cluster, l2.dirPath)
       case (_, _)                                          => unImplementedSyncOperation
     }

   def toRepository(data: Datasets, cluster: Cluster, repo: Repository): ResultTIO[Unit] = repo match {
     case LocalRepository(LocalIvoryLocation(l @ LocalLocation(path))) => getKeys(data).traverseU { key =>
       SyncHdfs.toLocal(cluster.root.location.dirPath </> FileName.unsafe(key.name), cluster, l.dirPath)
     }.void
     case _                                                           => unImplementedSyncOperation
   }

 }
