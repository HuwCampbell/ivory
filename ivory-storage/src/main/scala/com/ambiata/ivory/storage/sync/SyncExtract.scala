package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

object SyncExtract {

  def outputDataSet(input: ShadowOutputDataset, cluster: Cluster, output: OutputDataset): ResultTIO[Unit] =
    output.location.location match {
      case LocalLocation(_) =>
        unImplementedSyncOperation
      case S3Location(_, _) =>
        unImplementedSyncOperation
      case HdfsLocation(_) =>
        unImplementedSyncOperation
    }

  def toRepository(data: Datasets, cluster: Cluster, repo: Repository): ResultTIO[Unit] =
    unImplementedSyncOperation

}
