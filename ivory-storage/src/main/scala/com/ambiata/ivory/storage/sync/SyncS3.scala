package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core.Cluster
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FilePath

object SyncS3 {

  /**
   * Copy from (bucket, root) => cluster.root </> output
   */
  def toHdfs(bucket:String, root: FilePath, output:FilePath, cluster: Cluster): ResultTIO[Unit] =
    unImplementedSyncOperation

}
