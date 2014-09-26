package com.ambiata.ivory.core

import org.apache.hadoop.conf.Configuration

case class Cluster(root: IvoryLocation) {
  def ivory: IvoryConfiguration = root.ivory
  def hdfsConfiguration: Configuration = ivory.configuration
}
