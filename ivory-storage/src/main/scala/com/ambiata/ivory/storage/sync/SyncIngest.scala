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

     input.location match {
       case LocalLocation(path) =>
         val out =
           if (path.toFile.isFile) outputPath </> path.basename
           else                    outputPath

         SyncLocal.toHdfs(path, outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(out, cluster))

       case S3Location(path)     =>
         SyncS3.toHdfs(path, outputPath, cluster) >>
           ResultT.ok(shadowInputDatasetFromPath(outputPath, cluster))

       case HdfsLocation(path) =>
         ResultT.ok(ShadowInputDataset(path, cluster.hdfsConfiguration))
     }
   }

   def shadowInputDatasetFromPath(path: DirPath, cluster: Cluster): ShadowInputDataset =
     ShadowInputDataset(cluster.root </> path, cluster.ivory.configuration)

   def toCluster(datasets:Datasets, source: Repository, cluster: Cluster): ResultTIO[ShadowRepository] =
     (source match {
       case S3Repository(bucket, root, _) => getKeys(datasets).traverseU(key => SyncS3.toHdfs(root, DirPath.unsafe(key.name), cluster)).void
       case LocalRepository(root)         => getKeys(datasets).traverseU(key => SyncLocal.toHdfs(root, DirPath.unsafe(key.name), cluster)).void
       case HdfsRepository(_, _)          => ResultT.unit[IO]
     }).as(source match {
       case HdfsRepository(r, c) => ShadowRepository(r, c)
       case _                    => ShadowRepository(cluster.root, cluster.ivory)
     })

 }
