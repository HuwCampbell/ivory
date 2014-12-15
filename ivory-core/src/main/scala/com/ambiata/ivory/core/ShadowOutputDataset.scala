package com.ambiata.ivory.core

import com.ambiata.notion.core._
import org.apache.hadoop.fs.Path

case class ShadowOutputDataset(location: HdfsLocation) {
  def hdfsPath = new Path(location.path)
}

object ShadowOutputDataset {
  def fromIvoryLocation(loc: HdfsIvoryLocation): ShadowOutputDataset =
    ShadowOutputDataset(loc.location)

  def fromKey(key: Key): ShadowOutputDataset =
    ShadowOutputDataset(HdfsLocation(key.name))
}
