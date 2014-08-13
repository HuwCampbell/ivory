package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core.Name
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.mundane.io.BytesQuantity
import org.apache.hadoop.fs.Path

object Namespaces {
  /**
   * @return the list of namespaces for a given factset and their corresponding sizes
   */
  def namespaceSizes(factsetPath: Path): Hdfs[List[(Name, BytesQuantity)]] =
    Hdfs.childrenSizes(factsetPath).map(_.map { case (n, q) => (Name.fromPathName(n), q) })

  def namespaceSizesSingle(factsetPath: Path, namespace: Name): Hdfs[(Name, BytesQuantity)] =
    Hdfs.totalSize(factsetPath).map(namespace ->)

}
