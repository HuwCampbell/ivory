package com.ambiata.ivory.core

import com.ambiata.ivory.core.thrift._
import scala.collection.JavaConverters._

trait Fact {
  def entity: String
  def namespace: String
  def feature: String
  def featureId: FeatureId
  def date: Date
  def time: Time
  def datetime: DateTime
  def value: Value
  def toThrift: ThriftFact

  def toNamespacedThrift: NamespacedThriftFact with NamespacedThriftFactDerived

  def coordinateString(delim: Char): String = {
    val fields = List(s"$entity", s"$featureId", s"${date.int}-${time}}")
    fields.mkString(delim.toString)
  }

  def isTombstone: Boolean = value match {
    case v: TombstoneValue => true
    case _                  => false
  }

  def withEntity(newEntity: String): Fact =
    Fact.newFact(newEntity, namespace, feature, date, time, value)

  def withFeatureId(newFeatureId: FeatureId): Fact =
    Fact.newFact(entity, newFeatureId.namespace, newFeatureId.name, date, time, value)

  def withDate(newDate: Date): Fact =
    Fact.newFact(entity, namespace, feature, newDate, time, value)

  def withTime(newTime: Time): Fact =
    Fact.newFact(entity, namespace, feature, date, newTime, value)

  def withValue(newValue: Value): Fact =
    Fact.newFact(entity, namespace, feature, date, time, newValue)
}

object Fact {
  def newFact(entity: String, namespace: String, feature: String, date: Date, time: Time, value: Value): Fact =
    FatThriftFact.factWith(entity, namespace, feature, date, time, value match {
      case StringValue(s)   => ThriftFactValue.s(s)
      case BooleanValue(b)  => ThriftFactValue.b(b)
      case IntValue(i)      => ThriftFactValue.i(i)
      case LongValue(l)     => ThriftFactValue.l(l)
      case DoubleValue(d)   => ThriftFactValue.d(d)
      case TombstoneValue() => ThriftFactValue.t(new ThriftTombstone())
      case ListValue(v)     => ThriftFactValue.lst(new ThriftFactList(v.map {
        case p: PrimitiveValue  => ThriftFactListValue.p(primValue(p))
        case StructValue(m)     => ThriftFactListValue.s(new ThriftFactStructSparse(m.mapValues(primValue).asJava))
      }.asJava))
      case StructValue(m)   => ThriftFactValue.structSparse(new ThriftFactStructSparse(m.mapValues(primValue).asJava))
    })

  private def primValue(p: PrimitiveValue): ThriftFactPrimitiveValue = p match {
    // This duplication here is annoying/unfortunate - and will require a backwards-incompatible change
    case StringValue(s)   => ThriftFactPrimitiveValue.s(s)
    case BooleanValue(b)  => ThriftFactPrimitiveValue.b(b)
    case IntValue(i)      => ThriftFactPrimitiveValue.i(i)
    case LongValue(l)     => ThriftFactPrimitiveValue.l(l)
    case DoubleValue(d)   => ThriftFactPrimitiveValue.d(d)
    case TombstoneValue() => ThriftFactPrimitiveValue.t(new ThriftTombstone())
  }
}

trait NamespacedThriftFactDerived extends Fact { self: NamespacedThriftFact  =>
    def namespace: String =
      nspace

    def feature: String =
      fact.attribute

    def date: Date =
      Date.unsafeFromInt(yyyyMMdd)

    def time: Time =
      Time.unsafe(seconds)

    def datetime: DateTime =
      date.addTime(time)

    def entity: String =
      fact.getEntity

    def featureId: FeatureId =
      FeatureId(nspace, fact.getAttribute)

    def seconds: Int =
      Option(fact.getSeconds).getOrElse(0)

    def value: Value = fact.getValue match {
      case tv if tv.isSetD => DoubleValue(tv.getD)
      case tv if tv.isSetS => StringValue(tv.getS)
      case tv if tv.isSetI => IntValue(tv.getI)
      case tv if tv.isSetL => LongValue(tv.getL)
      case tv if tv.isSetB => BooleanValue(tv.getB)
      case tv if tv.isSetT => TombstoneValue()
      case tv if tv.isSetStructSparse
                           => StructValue(tv.getStructSparse.getV.asScala.toMap.mapValues(factPrimitiveToValue))
      case tv if tv.isSetLst
                           => ListValue(tv.getLst.getL.asScala.map {
        case l if l.isSetP => factPrimitiveToValue(l.getP)
        case l if l.isSetS => StructValue(l.getS.getV.asScala.toMap.mapValues(factPrimitiveToValue))
      }.toList)
      case _               => sys.error(s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${fact.toString}].'")
    }

    private def factPrimitiveToValue(v: ThriftFactPrimitiveValue): PrimitiveValue = v match {
      case tsv if tsv.isSetD => DoubleValue(tsv.getD)
      case tsv if tsv.isSetS => StringValue(tsv.getS)
      case tsv if tsv.isSetI => IntValue(tsv.getI)
      case tsv if tsv.isSetL => LongValue(tsv.getL)
      case tsv if tsv.isSetB => BooleanValue(tsv.getB)
      case tsv if tsv.isSetT => TombstoneValue()
      case _                 => sys.error(s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${fact.toString}].'")
    }

    def toThrift: ThriftFact = fact

    def toNamespacedThrift = this
}

object FatThriftFact {
  def apply(ns: String, date: Date, tfact: ThriftFact): Fact = new NamespacedThriftFact(tfact, ns, date.int) with NamespacedThriftFactDerived

  def factWith(entity: String, namespace: String, feature: String, date: Date, time: Time, value: ThriftFactValue): Fact = {
    val tfact = new ThriftFact(entity, feature, value)
    FatThriftFact(namespace, date, tfact.setSeconds(time.seconds))
  }
}

object BooleanFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Boolean): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.b(value))

  val fromTuple = apply _ tupled
}

object IntFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Int): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.i(value))

  val fromTuple = apply _ tupled
}

object LongFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Long): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.l(value))

  val fromTuple = apply _ tupled
}

object DoubleFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Double): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.d(value))

  val fromTuple = apply _ tupled
}

object StringFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: String): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.s(value))

  val fromTuple = apply _ tupled
}

object TombstoneFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time): Fact =
    FatThriftFact.factWith(entity, featureId.namespace, featureId.name, date, time, ThriftFactValue.t(new ThriftTombstone()))

  val fromTuple = apply _ tupled
}

sealed trait Value
sealed trait SubValue extends Value
sealed trait PrimitiveValue extends SubValue

case class BooleanValue(value: Boolean) extends PrimitiveValue
case class IntValue(value: Int) extends PrimitiveValue
case class LongValue(value: Long) extends PrimitiveValue
case class DoubleValue(value: Double) extends PrimitiveValue
case class StringValue(value: String) extends PrimitiveValue
case class TombstoneValue() extends PrimitiveValue

case class StructValue(values: Map[String, PrimitiveValue]) extends SubValue
case class ListValue(values: List[SubValue]) extends Value

object Value {
  def validDouble(d: Double): Boolean =
    !d.isNaN && !d.isNegInfinity && !d.isPosInfinity

  def toStringPrimitive(v: PrimitiveValue): Option[String] = v match {
    case BooleanValue(b)  => Some(b.toString)
    case IntValue(i)      => Some(i.toString)
    case LongValue(i)     => Some(i.toString)
    case DoubleValue(d)   => Some(d.toString)
    case StringValue(s)   => Some(s)
    case TombstoneValue() => None
  }

  def toString(v: Value, tombstoneValue: Option[String]): Option[String] = v match {
    case p: PrimitiveValue => toStringPrimitive(p) orElse tombstoneValue
    // Currently we're ignoring lists/structs in all text format (for now)
    case _: ListValue      => None
    case StructValue(_)    => None
  }

  /** This is _not_ for general consumption - should only be use for testing or diffing */
  def toStringWithStruct(v: Value): String = v match {
    case p: PrimitiveValue   => toStringPrimitive(p).getOrElse("")
    case ListValue(values)   => "[" + values.map(toStringWithStruct).mkString(",") + "]"
    case StructValue(values) =>
      "(" + values.map { case (k, p) => k + ":" + toStringPrimitive(p).getOrElse("")}.mkString(",") + ")"
  }

}
