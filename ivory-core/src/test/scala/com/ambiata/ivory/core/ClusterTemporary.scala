package com.ambiata.ivory.core

import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs._

case class ClusterTemporary() {
  def cluster: RIO[Cluster] = for {
    c <- IvoryConfigurationTemporary.random.conf
    p <- HdfsTemporary.random.path.run(c.configuration)
    r = Cluster.fromIvoryConfiguration(p, c, 1)
  } yield r
}

object ClusterTemporary {
  /** Deprecated callbacks. Use `ClusterTemporary.cluster` */
  def withCluster[A](f: Cluster => RIO[A]): RIO[A] = for {
    c <- ClusterTemporary().cluster
    r <- f(c)
  } yield r
}
