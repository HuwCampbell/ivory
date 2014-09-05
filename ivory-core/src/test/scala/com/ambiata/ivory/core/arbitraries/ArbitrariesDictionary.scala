package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._, Arbitraries._
import org.scalacheck.Arbitrary, Arbitrary._

/* Arbitraries related to Dictionary stuff */
trait ArbitrariesDictionary {

  /* Components that make up a dictionary that has _at least_ one virtual definition */
  case class VirtualDictionary(cd: ConcreteDefinition, fid: FeatureId, vd: VirtualDefinition) {

    def withSource(source: FeatureId): VirtualDictionary =
      copy(vd = vd.copy(source = source))

    def dictionary: Dictionary =
      Dictionary(List(cd.toDefinition(vd.source), vd.toDefinition(fid)))
  }

  case class VirtualDictionaryWindow(vdict: VirtualDictionary, window: Window) {
    def vd: VirtualDictionary =
      vdict.copy(vd = vdict.vd.copy(window = Some(window)))
  }

  implicit def VirtualDictionaryArbitrary: Arbitrary[VirtualDictionary] = Arbitrary(for {
    cd  <- arbitrary[ConcreteDefinition]
    fid <- arbitrary[FeatureId]
    vd  <- virtualDefGen(fid -> cd)
  } yield VirtualDictionary(cd, vd._1, vd._2))

  implicit def VirtualDictionaryWithWindow: Arbitrary[VirtualDictionaryWindow] = Arbitrary(for {
    vd <- arbitrary[VirtualDictionary]
    w  <- arbitrary[Window]
  } yield VirtualDictionaryWindow(vd, w))
}
