package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.alien.hdfs.Hdfs
import com.ambiata.mundane.io.BytesQuantity
import org.apache.hadoop.fs.Path

object Namespaces {
  /**
   * @return the list of namespaces for a given factset and their corresponding sizes
   *         If a single namespace is passed, the input path is interpreted as the directory for a
   *         single namespace being named <singleNamespace>
   */
  def namespaceSizes(factsetPath: Path, singleNamespace: Option[String]): Hdfs[List[(String, BytesQuantity)]] =
    singleNamespace match {
      case Some(name) => Hdfs.totalSize(factsetPath).map(size => List((name, size)))
      case None       => Hdfs.childrenSizes(factsetPath).map(_.map { case (n, q) => (n.getName, q) })
    }

  /**
   * @return the list of partitions for a given factset and their corresponding sizes
   */
  def partitionSizes(factsetPath: Path): Hdfs[List[(Path, BytesQuantity)]] =
    Hdfs.childrenSizes(factsetPath, "*/*/*/*").map(_.filterNot { case (n, q) =>
      n.getName.startsWith("_") || n.getName.startsWith(".")
    })

}
