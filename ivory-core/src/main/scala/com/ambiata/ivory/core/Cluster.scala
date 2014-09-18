package com.ambiata.ivory.core

import com.ambiata.mundane.io.FilePath
import org.apache.hadoop.conf.Configuration

case class Cluster(root: FilePath, ivory: IvoryConfiguration) {
  def hdfsConfiguration: Configuration = ivory.configuration
}
