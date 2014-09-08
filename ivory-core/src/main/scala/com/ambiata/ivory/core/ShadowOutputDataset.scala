package com.ambiata.ivory.core

import com.ambiata.mundane.io.FilePath
import org.apache.hadoop.conf.Configuration

case class ShadowOutputDataset(location: FilePath, conf: Configuration)