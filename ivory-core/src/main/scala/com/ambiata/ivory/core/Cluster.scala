package com.ambiata.ivory.core

import org.apache.hadoop.conf.Configuration

case class Cluster(root: HdfsIvoryLocation) {
  def hdfsConfiguration: Configuration = root.configuration
}
