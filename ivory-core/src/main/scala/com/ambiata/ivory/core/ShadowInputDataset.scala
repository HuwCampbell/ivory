package com.ambiata.ivory.core

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.mundane.io.DirPath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class ShadowInputDataset(path: DirPath, configuration: Configuration) {
  def toHdfsPath: Path = path.toHdfs

}
