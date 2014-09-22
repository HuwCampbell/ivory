package com.ambiata.ivory.core

import com.ambiata.mundane.io._
import org.apache.hadoop.conf.Configuration

case class ShadowOutputDataset(path: DirPath, conf: Configuration)