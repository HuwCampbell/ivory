package com.ambiata.ivory.core
package arbitraries

import org.scalacheck._, Arbitrary._
import ArbitraryFeatures._
import scalaz._, Scalaz._, scalacheck.ScalaCheckBinding._

/**
 *  Dictionary arbitraries
 */
trait ArbitraryDictionaries {

  implicit def VirtualDictionaryArbitrary: Arbitrary[VirtualDictionary] = Arbitrary(for {
    cd  <- arbitrary[ConcreteDefinition]
    fid <- arbitrary[FeatureId]
    vd  <- virtualDefGen(fid -> cd)
  } yield VirtualDictionary(cd, vd._1, vd._2))

  implicit def VirtualDictionaryWithWindow: Arbitrary[VirtualDictionaryWindow] = Arbitrary(for {
    vd <- arbitrary[VirtualDictionary]
    w  <- arbitrary[Window]
  } yield VirtualDictionaryWindow(vd, w))

  implicit def DictionaryArbitrary: Arbitrary[Dictionary] =
    Arbitrary(for {
      n <- Gen.choose(10, 20)
      i <- Gen.listOfN(n, arbitrary[FeatureId]).map(_.distinct)
      c <- Gen.listOfN(i.length, arbitrary[ConcreteDefinition]).map(cds => i.zip(cds))
      // For every concrete definition there is a chance we may have a virtual feature
      v <- c.traverse(x => Gen.frequency(
        70 -> Gen.const(None),
        30 -> virtualDefGen(x).map(some).map(_.filterNot(vd => i.contains(vd._1))))
      ).map(_.flatten)
    } yield Dictionary(c.map({ case (f, d) => d.toDefinition(f) }) ++ v.map({ case (f, d) => d.toDefinition(f) })))

  implicit def DictIdArbitrary: Arbitrary[DictId] = Arbitrary(
    GenPlus.nonEmptyListOf(Gen.frequency(
      1 -> Gen.const("_"),
      99 -> Gen.alphaNumChar
    )).map(_.mkString).map(DictId)
  )

  implicit def DictTombArbitrary: Arbitrary[DictTomb] =
    Arbitrary(arbitrary[DictDesc].map(_.s).retryUntil(s => !s.contains(",") && !s.contains("\"")).map(DictTomb))

  implicit def DictDescArbitrary: Arbitrary[DictDesc] =
    Arbitrary(Gen.identifier.map(_.trim).retryUntil(s => !s.contains("|") && !s.contains("\uFFFF") && s.forall(_ > 31)).map(DictDesc))
}

object ArbitraryDictionaries extends ArbitraryDictionaries

/** Components that make up a dictionary that has _at least_ one virtual definition */
case class VirtualDictionary(cd: ConcreteDefinition, fid: FeatureId, vd: VirtualDefinition) {

  def withExpression(expression: Expression): VirtualDictionary =
    copy(vd = vd.copy(query = vd.query.copy(expression = expression)))

  def withSource(source: FeatureId): VirtualDictionary =
    copy(vd = vd.copy(source = source))

  def dictionary: Dictionary =
    Dictionary(List(cd.toDefinition(vd.source), vd.toDefinition(fid)))
}

/** identifier for a dictionary */
case class DictId(s: String)
/** description for a dictionary */
case class DictDesc(s: String)
/** tombstone for a dictionary */
case class DictTomb(s: String)

case class VirtualDictionaryWindow(vdict: VirtualDictionary, window: Window) {
  def vd: VirtualDictionary =
    vdict.copy(vd = vdict.vd.copy(window = Some(window)))
}


