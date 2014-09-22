package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import org.apache.hadoop.conf.Configuration

case class Cluster(root: DirPath, ivory: IvoryConfiguration) {
  def hdfsConfiguration: Configuration = ivory.configuration
}
