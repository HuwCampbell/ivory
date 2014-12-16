package com.ambiata.ivory.core

import com.ambiata.notion.core._

case class OutputDataset(location: Location)

object OutputDataset {
  def fromIvoryLocation(loc: IvoryLocation): OutputDataset =
    OutputDataset(loc.location)

}
