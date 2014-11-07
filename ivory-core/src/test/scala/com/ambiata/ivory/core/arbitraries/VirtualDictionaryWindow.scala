package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._

import org.scalacheck._, Arbitrary._
import Arbitraries._


case class VirtualDictionaryWindow(vdict: VirtualDictionary, window: Window) {
  def vd: VirtualDictionary =
    vdict.copy(vd = vdict.vd.copy(window = Some(window)))
}

object VirtualDictionaryWindow {
  implicit def VirtualDictionaryWithWindow: Arbitrary[VirtualDictionaryWindow] = Arbitrary(for {
    vd <- arbitrary[VirtualDictionary]
    w  <- arbitrary[Window]
  } yield VirtualDictionaryWindow(vd, w))
}
