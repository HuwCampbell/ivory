package com.ambiata.ivory.core

import com.ambiata.mundane.io._

case class OutputDataset(location: Location) {
  def path: FilePath = location.path
}