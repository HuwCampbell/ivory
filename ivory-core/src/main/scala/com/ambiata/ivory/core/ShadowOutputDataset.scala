package com.ambiata.ivory.core

import com.ambiata.notion.core._
import org.apache.hadoop.fs.Path

case class ShadowOutputDataset(location: HdfsLocation) {
  def hdfsPath = new Path(location.path)
}

object ShadowOutputDataset {
  def fromIvoryLocation(loc: HdfsIvoryLocation): ShadowOutputDataset =
    ShadowOutputDataset(loc.location)

  def fromIvoryLocationUnsafe(loc: IvoryLocation): ShadowOutputDataset = loc match {
    case HdfsIvoryLocation(h, _, _, _) =>
      ShadowOutputDataset(h)
    case _ =>
      Crash.error(Crash.CodeGeneration, "Only HdfsIvoryLocations are supported at this time") // Until plan is implemented
  }

  def fromKey(key: Key): ShadowOutputDataset =
    ShadowOutputDataset(HdfsLocation(key.name))
}
