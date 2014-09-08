package com.ambiata.ivory.core

import com.ambiata.mundane.io._

case class InputDataset(location: Location) {
  def path: FilePath = location.path
}