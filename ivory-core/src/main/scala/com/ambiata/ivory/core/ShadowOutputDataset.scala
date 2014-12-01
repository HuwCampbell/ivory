package com.ambiata.ivory.core

import com.ambiata.notion.core._

case class ShadowOutputDataset(location: HdfsLocation)

object ShadowOutputDataset {
  def fromIvoryLocation(loc: HdfsIvoryLocation): ShadowOutputDataset =
    ShadowOutputDataset(loc.location)
}
