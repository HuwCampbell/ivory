package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.alien.hdfs.Hdfs
import com.ambiata.mundane.io.BytesQuantity
import org.apache.hadoop.fs.Path

object Namespaces {
  /**
   * @return the list of namespaces for a given factset and their corresponding sizes
   */
  def namespaceSizes(factsetPath: Path): Hdfs[List[(String, BytesQuantity)]] =
    Hdfs.childrenSizes(factsetPath).map(_.collect { case (n, q) if !Seq("_", ".").exists(n.getName.startsWith) =>
      (n.getName, q)
    })

  /**
   * @return the list of partitions for a given factset and their corresponding sizes
   */
  def partitionSizes(factsetPath: Path): Hdfs[List[(Path, BytesQuantity)]] =
    Hdfs.childrenSizes(factsetPath, "*/*/*/*").map(_.filterNot { case (n, q) =>
      n.getName.startsWith("_") || n.getName.startsWith(".")
    })

}
