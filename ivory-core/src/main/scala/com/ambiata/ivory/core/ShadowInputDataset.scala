package com.ambiata.ivory.core

import org.apache.hadoop.conf.Configuration

case class ShadowInputDataset(location: HdfsIvoryLocation) {
  def configuration: Configuration = location.configuration
}