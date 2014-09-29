package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._, Arbitraries._
import org.scalacheck._, Arbitrary._

/* Arbitraries related to Dictionary stuff */
trait ArbitrariesDictionary {

  /* Components that make up a dictionary that has _at least_ one virtual definition */
  case class VirtualDictionary(cd: ConcreteDefinition, fid: FeatureId, vd: VirtualDefinition) {

    def withExpression(expression: Expression): VirtualDictionary =
      copy(vd = vd.copy(query = vd.query.copy(expression = expression)))

    def withSource(source: FeatureId): VirtualDictionary =
      copy(vd = vd.copy(source = source))

    def dictionary: Dictionary =
      Dictionary(List(cd.toDefinition(vd.source), vd.toDefinition(fid)))
  }

  /** Helpful wrapper around [[ConcreteGroup]] */
  case class ConcreteGroupFeature(fid: FeatureId, cg: ConcreteGroup) {

    def withExpression(expression: Expression): ConcreteGroupFeature =
      copy(cg = cg.copy(virtual = cg.virtual.map(vd => vd._1 -> vd._2.copy(query = vd._2.query.copy(expression = expression)))))

    def dictionary: Dictionary =
      Dictionary(cg.definition.toDefinition(fid) :: cg.virtual.map(vd => vd._2.toDefinition(vd._1)))
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

  implicit def ConcreteGroupFeatureArbitrary: Arbitrary[ConcreteGroupFeature] = Arbitrary(for {
    fid <- arbitrary[FeatureId]
    cd  <- arbitrary[ConcreteDefinition]
    vi <- Gen.choose(0, 3)
    vd <- Gen.listOfN(vi, virtualDefGen(fid -> cd))
  } yield ConcreteGroupFeature(fid, ConcreteGroup(cd, vd.toMap.mapValues(_.copy(source = fid)).toList)))

  implicit def VirtualDictionaryWithWindow: Arbitrary[VirtualDictionaryWindow] = Arbitrary(for {
    vd <- arbitrary[VirtualDictionary]
    w  <- arbitrary[Window]
  } yield VirtualDictionaryWindow(vd, w))
}
