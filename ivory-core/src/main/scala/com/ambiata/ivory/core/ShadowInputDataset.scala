package com.ambiata.ivory.core

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.mundane.io.FilePath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class ShadowInputDataset(location: FilePath, conf: Configuration) {
  def toHdfsPath: Path = location.toHdfs

  def toFilePath: FilePath = location

  def configuration: Configuration = conf

}
