package com.ambiata.ivory.storage.sync

import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.saws.s3.{S3Path, S3}

import scalaz._, Scalaz._, effect.IO

object SyncIngest {

   def inputDataSet(input: InputDataset, cluster: Cluster): ResultTIO[ShadowInputDataset] = {
     val outputPath = DirPath.unsafe(s"shadowInputDataset/${UUID.randomUUID()}")

     input.location.location match {
       case l: LocalLocation =>
         val out =
           if (l.path.toFile.isFile) outputPath </> l.path.basename
           else                      outputPath

         SyncLocal.toHdfs(l.path, outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(out, cluster))

       case l: S3Location =>
         SyncS3.toHdfs(l.path, outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(outputPath, cluster))

       case l: HdfsLocation =>
         ResultT.ok(ShadowInputDataset(input.location.copy(ivory = cluster.ivory)))
     }
   }

   def shadowInputDatasetFromPath(path: DirPath, cluster: Cluster): ShadowInputDataset =
     ShadowInputDataset((cluster.root </> path).copy(ivory = cluster.ivory))

   def toCluster(datasets:Datasets, source: Repository, cluster: Cluster): ResultTIO[ShadowRepository] =
     (source match {
       case S3Repository(bucket, root, _) => getKeys(datasets).traverseU(key => SyncS3.toHdfs(root, DirPath.unsafe(key.name), cluster)).void
       case LocalRepository(root)         => getKeys(datasets).traverseU(key => SyncLocal.toHdfs(root.path, DirPath.unsafe(key.name), cluster)).void
       case HdfsRepository(_, _)          => ResultT.unit[IO]
     }).as(source match {
       case HdfsRepository(r, c) => ShadowRepository(IvoryLocation(r, c))
       case _                    => ShadowRepository(cluster.root)
     })

 }
