package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core.Cluster
import com.ambiata.ivory.core.NotImplemented._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

object SyncS3 {

  /**
   * Copy from (bucket, root) => cluster.root </> output
   */
  def toHdfs(root: DirPath, output:DirPath, cluster: Cluster): ResultTIO[Unit] =
    unImplementedSyncOperation

}
