package com.ambiata.ivory.core

import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.arbitraries._, Arbitraries._
import org.specs2.{ScalaCheck, Specification}
import scalaz.scalacheck.ScalazProperties

class ValueSpec extends Specification with ScalaCheck { def is = s2"""

  Can validate with correct encoding                     $valid
  Can validate with incorrect encoding                   $invalid
  Can convert to and from thrift                         $thrift
  Can convert between primitive and non-primitive        $primitive
  Can make any value unique                              $unique
  Can make any value unique                              $order
  Can make any value unique                              $orderPrimitive
"""

  def valid = prop((e: EncodingAndValue) =>
    Value.validateEncoding(e.value, e.enc).toEither must beRight
  )

  def invalid = prop((e: EncodingAndValue, e2: Encoding) => (e.enc != e2 && e.value != TombstoneValue && !isCompatible(e, e2)) ==> {
    Value.validateEncoding(e.value, e2).toEither must beLeft
  })

  def thrift = prop { (e: EncodingAndValue) =>
    Value.fromThrift(Value.toThrift(e.value)) ==== e.value
  }

  def primitive = prop { (v: PrimitiveValue) =>
    Value.toPrimitive(Value.toThrift(v)).map(Value.fromPrimitive) must beSome(Value.toThrift(v))
  }

  def unique = prop((v: EncodingAndValue, i: Int) => (i != 0 && canBeMadeUnique(v.value)) ==> {
    Value.unique(v.value, i) !=== v.value
  })

  def order =
    ScalazProperties.order.laws[Value](Value.order, Arbitraries.ValueArbitrary)

  def orderPrimitive =
    ScalazProperties.order.laws[PrimitiveValue](Value.orderPrimitive, Arbitraries.PrimitiveValueArbitrary)

  // A small subset of  encoded values are valid for different optional/empty Structs/Lists
  private def isCompatible(e1: EncodingAndValue, e2: Encoding): Boolean =
    (e1, e2) match {
      case (EncodingAndValue(_, StructValue(m)), EncodingStruct(StructEncoding(v))) =>
        m.isEmpty && v.forall(_._2.optional)
      case (EncodingAndValue(_, ListValue(l)), EncodingList(ListEncoding(e))) =>
        l.forall(v => isCompatible(EncodingAndValue(e.toEncoding, v), e.toEncoding))
      case _ => false
    }

  def canBeMadeUnique(v: Value): Boolean = {
    def canBeMadeUniquePrim(pv: PrimitiveValue): Boolean = pv match {
      // Yeah good luck with that
      case BooleanValue(_) => false
      case DoubleValue(d) => d + 1 != d
      case _ => true
    }
    v match {
      case TombstoneValue => false
      case pv: PrimitiveValue => canBeMadeUniquePrim(pv)
      case ListValue(lv) => lv.headOption.exists {
        case pv: PrimitiveValue => canBeMadeUniquePrim(pv)
        case StructValue(sv) => sv.values.headOption.exists(canBeMadeUniquePrim)
      }
      case StructValue(sv) => sv.values.headOption.exists(canBeMadeUniquePrim)
      case _ => true
    }
  }
}
