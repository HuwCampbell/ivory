package com.ambiata.ivory.core

import org.apache.hadoop.conf.Configuration

case class ShadowInputDataset(location: IvoryLocation) {
  def configuration: Configuration = location.configuration
}