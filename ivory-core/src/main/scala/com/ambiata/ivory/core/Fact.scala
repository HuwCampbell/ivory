package com.ambiata.ivory.core

import com.ambiata.ivory.core.thrift._
import scala.collection.JavaConverters._
import scalaz._, Scalaz._

trait Fact {
  def entity: String
  def namespace: Namespace
  /** fast access method for the Fact Namespace which does not validate the namespace string */
  def namespaceUnsafe: Namespace
  def feature: String
  def featureId: FeatureId
  def date: Date
  def time: Time
  def datetime: DateTime
  def value: Value
  def toThrift: ThriftFact

  def partition: Partition =
    Partition(namespace, date)

  def toNamespacedThrift: MutableFact

  def coordinateString(delim: Char): String = {
    val fields = List(s"$entity", s"$featureId", s"${date.int}-${time}")
    fields.mkString(delim.toString)
  }

  def isTombstone: Boolean = value match {
    case TombstoneValue     => true
    case _                  => false
  }

  def withEntity(newEntity: String): Fact =
    Fact.newFactWithNamespace(newEntity, namespace, feature, date, time, value)

  def withFeatureId(newFeatureId: FeatureId): Fact =
    Fact.newFactWithNamespace(entity, newFeatureId.namespace, newFeatureId.name, date, time, value)

  def withDate(newDate: Date): Fact =
    Fact.newFactWithNamespace(entity, namespace, feature, newDate, time, value)

  def withTime(newTime: Time): Fact =
    Fact.newFactWithNamespace(entity, namespace, feature, date, newTime, value)

  def withDateTime(newDateTime: DateTime): Fact =
    Fact.newFactWithNamespace(entity, namespace, feature, newDateTime.date, newDateTime.time, value)

  def withValue(newValue: Value): Fact =
    Fact.newFactWithNamespace(entity, namespace, feature, date, time, newValue)
}

object Fact {
  def newFactWithNamespace(entity: String, namespace: Namespace, feature: String, date: Date, time: Time, value: Value): Fact =
    newFact(entity, namespace.name, feature, date, time, value)

  def newFact(entity: String, namespace: String, feature: String, date: Date, time: Time, value: Value): Fact =
    FatThriftFact.factWith(entity, namespace, feature, date, time, Value.toThrift(value))

  /** For testing only! */
  def orderEntityDateTime: Order[Fact] =
    Order.orderBy[Fact, (String, Long)](f => f.entity -> f.datetime.long) |+|
      // We need to sort by value too for sets where the same entity/datetime exists
      Order.order { case (f1, f2) => Value.order(f1.value, f2.value) }
}

trait NamespacedThriftFactDerived extends Fact { self: NamespacedThriftFact  =>

    def namespace: Namespace =
      // this name should be well-formed if the ThriftFact has been validated
      // if that's not the case an exception will be thrown here
      Namespace.reviewed(nspace)

    def namespaceUnsafe: Namespace =
      Namespace.unsafe(nspace)

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
      FeatureId(namespace, fact.getAttribute)

    def seconds: Int =
      Option(fact.getSeconds).getOrElse(0)

    def value: Value =
      Value.fromThrift(fact.getValue)

    def toThrift: ThriftFact = fact

    def toNamespacedThrift = this

    // Overriding for performance to avoid extra an extra allocation
    override def isTombstone: Boolean =
      fact.getValue.isSetT
}

object FatThriftFact {
  def apply(ns: String, date: Date, tfact: ThriftFact): MutableFact =
    new NamespacedThriftFact(tfact, ns, date.int) with NamespacedThriftFactDerived

  def factWith(entity: String, namespace: String, feature: String, date: Date, time: Time, value: ThriftFactValue): Fact = {
    val tfact = new ThriftFact(entity, feature, value)
    FatThriftFact(namespace, date, if (time.seconds != 0) tfact.setSeconds(time.seconds) else tfact)
  }
}

object BooleanFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Boolean): Fact =
    FatThriftFact.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.b(value))

  val fromTuple = apply _ tupled
}

object IntFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Int): Fact =
    FatThriftFact.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.i(value))

  val fromTuple = apply _ tupled
}

object LongFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Long): Fact =
    FatThriftFact.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.l(value))

  val fromTuple = apply _ tupled
}

object DoubleFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Double): Fact =
    FatThriftFact.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.d(value))

  val fromTuple = apply _ tupled
}

object StringFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: String): Fact =
    FatThriftFact.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.s(value))

  val fromTuple = apply _ tupled
}

object DateFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Date): Fact =
    FatThriftFact.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.date(value.int))
}

object TombstoneFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time): Fact =
    FatThriftFact.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.t(new ThriftTombstone()))

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
case class DateValue(value: Date) extends PrimitiveValue
case object TombstoneValue extends Value

case class StructValue(values: Map[String, PrimitiveValue]) extends SubValue
case class ListValue(values: List[SubValue]) extends Value

object Value {

  val MaxEntityLength: Int = 256

  def validDouble(d: Double): Boolean =
    !d.isNaN && !d.isNegInfinity && !d.isPosInfinity

  def toStringPrimitive(v: PrimitiveValue): String = v match {
    case BooleanValue(b)  => b.toString
    case IntValue(i)      => i.toString
    case LongValue(i)     => i.toString
    case DoubleValue(d)   => d.toString
    case DateValue(r)     => r.hyphenated
    case StringValue(s)   => s
  }

  def toString(v: Value, tombstoneValue: Option[String]): Option[String] = v match {
    case TombstoneValue    => tombstoneValue
    case p: PrimitiveValue => Some(toStringPrimitive(p))
    // Currently we're ignoring lists/structs in all text format (for now)
    case _: ListValue      => None
    case StructValue(_)    => None
  }

  def toStringOr(v: Value, tombstoneValue: String): Option[String] =
    toString(v, Some(tombstoneValue))

  /** This is _not_ for general consumption - should only be use for testing or diffing */
  def toStringWithStruct(v: Value, tombstone: String): String = v match {
    case TombstoneValue      => tombstone
    case p: PrimitiveValue   => toStringPrimitive(p)
    case ListValue(values)   => "[" + values.map(toStringWithStruct(_, tombstone)).mkString(",") + "]"
    case StructValue(values) =>
      "(" + values.map { case (k, p) => k + ":" + toStringPrimitive(p)}.mkString(",") + ")"
  }

  def parsePrimitive(encoding: PrimitiveEncoding, raw: String): Validation[String, PrimitiveValue] = encoding match {
    case BooleanEncoding                         => raw.parseBoolean.leftMap(_ => s"Value '$raw' is not a boolean").map(v => BooleanValue(v))
    case IntEncoding                             => raw.parseInt.leftMap(_ => s"Value '$raw' is not an integer").map(v => IntValue(v))
    case LongEncoding                            => raw.parseLong.leftMap(_ => s"Value '$raw' is not a long").map(v => LongValue(v))
    case DoubleEncoding                          => raw.parseDouble.ensure(())(Value.validDouble)
      .leftMap(_ => s"Value '$raw' is not a double").map(v => DoubleValue(v))
    case DateEncoding                            => Dates.date(raw).toRightDisjunction(s"Value '$raw' is not a date").validation.map(v => DateValue(v))
    case StringEncoding                          => StringValue(raw).success[String]
  }

  def validateFact(fact: Fact, dict: Dictionary): Validation[String, Fact] =
    validateEntity(fact) match {
      case Success(f) =>
        dict.byFeatureId.get(f.featureId).map({
          case Concrete(_, fm) => validateEncoding(f.value, fm.encoding).as(f).leftMap(_ + s" '${f.toString}'")
          case _: Virtual      => s"Cannot have virtual facts for ${f.featureId}".failure
        }).getOrElse(s"Dictionary entry '${f.featureId}' doesn't exist!".failure)
      case e @ Failure(_) =>
        e
    }

  def validateEntity(fact: Fact): Validation[String, Fact] =
    if(fact.entity.length > MaxEntityLength) Failure(s"Entity id '${fact.entity}' too large! Max length is ${MaxEntityLength}") else Success(fact)

  def validateEncoding(value: Value, encoding: Encoding): Validation[String, Unit] = {
    def fail: Validation[String, Unit] =
      s"Not a valid ${Encoding.render(encoding)}!".failure
    def validateEncodingPrim(v: PrimitiveValue, e: PrimitiveEncoding): Validation[String, Unit] = (v, e) match {
      case (BooleanValue(_), BooleanEncoding)   => Success(())
      case (BooleanValue(_), _)                 => fail
      case (IntValue(_),     IntEncoding)       => Success(())
      case (IntValue(_),     _)                 => fail
      case (LongValue(_),    LongEncoding)      => Success(())
      case (LongValue(_),    _)                 => fail
      case (DoubleValue(_),  DoubleEncoding)    => Success(())
      case (DoubleValue(_),  _)                 => fail
      case (StringValue(_),  StringEncoding)    => Success(())
      case (StringValue(_),  _)                 => fail
      case (DateValue(_),    DateEncoding)      => Success(())
      case (DateValue(_),    _)                 => fail
    }
    def validateEncodingSub(v: SubValue, e: SubEncoding): Validation[String, Unit] = (v, e) match {
      case (vp: PrimitiveValue, SubPrim(ep))    => validateEncodingPrim(vp, ep)
      case (vp: PrimitiveValue, _)              => fail
      case (StructValue(vs),  SubStruct(es))    => validateStruct(vs, es)
      case (StructValue(_),  _)                 => fail
    }
    def validateStruct(values: Map[String, PrimitiveValue], encoding: StructEncoding): Validation[String, Unit] =
      Maps.outerJoin(encoding.values, values).toStream.foldMap {
        case (n, \&/.This(enc))        => if (!enc.optional) s"Missing struct $n".failure else ().success
        case (n, \&/.That(_))          => s"Undeclared struct value $n".failure
        case (n, \&/.Both(enc, v))     => validateEncodingPrim(v, enc.encoding).leftMap(_ + s" for $n")
      }
    (value, encoding) match {
      case (TombstoneValue,  _)                 => Success(())
      case (vp: PrimitiveValue, EncodingPrim(ep)) => validateEncodingPrim(vp, ep)
      case (vp: PrimitiveValue, _)              => fail
      case (StructValue(vs), EncodingStruct(es)) => validateStruct(vs, es)
      case (StructValue(_),  _)                 => fail
      case (ListValue(v), EncodingList(l))      => v.foldMap(validateEncodingSub(_, l.encoding))
      case (ListValue(_),    _)                 => fail
    }
  }

  def toThrift(value: Value): ThriftFactValue =
    value match {
      case StringValue(s)   => ThriftFactValue.s(s)
      case BooleanValue(b)  => ThriftFactValue.b(b)
      case IntValue(i)      => ThriftFactValue.i(i)
      case LongValue(l)     => ThriftFactValue.l(l)
      case DoubleValue(d)   => ThriftFactValue.d(d)
      case TombstoneValue   => ThriftFactValue.t(new ThriftTombstone())
      case DateValue(r)     => ThriftFactValue.date(r.int)
      case ListValue(v)     => ThriftFactValue.lst(new ThriftFactList(v.map {
        case p: PrimitiveValue  => ThriftFactListValue.p(toThriftPrimitive(p))
        case StructValue(m)     => ThriftFactListValue.s(new ThriftFactStructSparse(m.mapValues(toThriftPrimitive).asJava))
      }.asJava))
      case StructValue(m)   => ThriftFactValue.structSparse(new ThriftFactStructSparse(m.mapValues(toThriftPrimitive).asJava))
    }

  def toThriftPrimitive(p: PrimitiveValue): ThriftFactPrimitiveValue = p match {
    // This duplication here is annoying/unfortunate - and will require a backwards-incompatible change
    case StringValue(s)   => ThriftFactPrimitiveValue.s(s)
    case BooleanValue(b)  => ThriftFactPrimitiveValue.b(b)
    case IntValue(i)      => ThriftFactPrimitiveValue.i(i)
    case LongValue(l)     => ThriftFactPrimitiveValue.l(l)
    case DoubleValue(d)   => ThriftFactPrimitiveValue.d(d)
    case DateValue(r)     => ThriftFactPrimitiveValue.date(r.int)
  }

  def fromThrift(value: ThriftFactValue): Value = {
    value match {
      case tv if tv.isSetD => DoubleValue(tv.getD)
      case tv if tv.isSetS => StringValue(tv.getS)
      case tv if tv.isSetI => IntValue(tv.getI)
      case tv if tv.isSetL => LongValue(tv.getL)
      case tv if tv.isSetB => BooleanValue(tv.getB)
      case tv if tv.isSetDate => DateValue(Date.unsafeFromInt(tv.getDate))
      case tv if tv.isSetT => TombstoneValue
      case tv if tv.isSetStructSparse
                           => StructValue(tv.getStructSparse.getV.asScala.toMap.mapValues(fromThriftPrimitive))
      case tv if tv.isSetLst
                           => ListValue(tv.getLst.getL.asScala.map {
        case l if l.isSetP => fromThriftPrimitive(l.getP)
        case l if l.isSetS => StructValue(l.getS.getV.asScala.toMap.mapValues(fromThriftPrimitive))
      }.toList)
      case _               => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${value.toString}].'")
    }
  }

  def fromThriftPrimitive(v: ThriftFactPrimitiveValue): PrimitiveValue = v match {
    case tsv if tsv.isSetD => DoubleValue(tsv.getD)
    case tsv if tsv.isSetS => StringValue(tsv.getS)
    case tsv if tsv.isSetI => IntValue(tsv.getI)
    case tsv if tsv.isSetL => LongValue(tsv.getL)
    case tsv if tsv.isSetB => BooleanValue(tsv.getB)
    case tsv if tsv.isSetDate => DateValue(Date.unsafeFromInt(tsv.getDate))
    case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
  }

  def toPrimitive(value: ThriftFactValue): Option[ThriftFactPrimitiveValue] = value match {
    case tv if tv.isSetD => Some(ThriftFactPrimitiveValue.d(tv.getD))
    case tv if tv.isSetS => Some(ThriftFactPrimitiveValue.s(tv.getS))
    case tv if tv.isSetI => Some(ThriftFactPrimitiveValue.i(tv.getI))
    case tv if tv.isSetL => Some(ThriftFactPrimitiveValue.l(tv.getL))
    case tv if tv.isSetB => Some(ThriftFactPrimitiveValue.b(tv.getB))
    case tv if tv.isSetDate => Some(ThriftFactPrimitiveValue.date(tv.getDate))
    case _               => None
  }

  def fromPrimitive(value: ThriftFactPrimitiveValue): ThriftFactValue = value match {
    case tsv if tsv.isSetD => ThriftFactValue.d(tsv.getD)
    case tsv if tsv.isSetS => ThriftFactValue.s(tsv.getS)
    case tsv if tsv.isSetI => ThriftFactValue.i(tsv.getI)
    case tsv if tsv.isSetL => ThriftFactValue.l(tsv.getL)
    case tsv if tsv.isSetB => ThriftFactValue.b(tsv.getB)
    case tsv if tsv.isSetDate => ThriftFactValue.date(tsv.getDate)
  }

  /** For testing - make a single value a little more unique based on an offset */
  def unique(value: Value, i: Int): Value = value match {
    case v: PrimitiveValue => uniquePrim(v, i)
    case StructValue(v) => StructValue(v.mapValues(uniquePrim(_, i)))
    case ListValue(lv) => ListValue(lv.map({
      case v: PrimitiveValue => uniquePrim(v, i)
      case StructValue(v) => StructValue(v.mapValues(uniquePrim(_, i)))
    }))
    case TombstoneValue => TombstoneValue
  }

  def uniquePrim(value: PrimitiveValue, i: Int): PrimitiveValue = value match {
    case StringValue(v) => StringValue(v + "_" + i)
    case BooleanValue(_) => BooleanValue(i % 2 == 0)
    case IntValue(v) => IntValue(v + i)
    case LongValue(v) => LongValue(v + i)
    case DoubleValue(v) => DoubleValue(v + i)
    case DateValue(v) => DateValue(DateTimeUtil.minusDays(v, i))
  }

  /** For testing only! */
  def order: Order[Value] =
    Order.order {
      case (TombstoneValue, TombstoneValue) => Ordering.EQ
      case (v1: PrimitiveValue, v2: PrimitiveValue) => Value.orderPrimitive.order(v1, v2)
      case (StructValue(v1), StructValue(v2)) => mapOrder(stringInstance, orderPrimitive)(v1, v2)
      case (ListValue(lv1), ListValue(lv2)) => listOrder(Order.order[SubValue] {
        case (v1: PrimitiveValue, v2: PrimitiveValue) => Value.orderPrimitive.order(v1, v2)
        case (StructValue(v1), StructValue(v2)) => mapOrder(stringInstance, orderPrimitive)(v1, v2)
        case (_: PrimitiveValue, _) => Ordering.LT
        case (_, _: PrimitiveValue) => Ordering.GT
        case (StructValue(_), _) => Ordering.LT
        case (_, StructValue(_)) => Ordering.GT
      })(lv1, lv2)
      case (TombstoneValue, _) => Ordering.LT
      case (_, TombstoneValue) => Ordering.GT
      case (_: PrimitiveValue, _) => Ordering.LT
      case (_, _: PrimitiveValue) => Ordering.GT
      case (StructValue(_), _) => Ordering.LT
      case (_, StructValue(_)) => Ordering.GT
      case (ListValue(_), _) => Ordering.LT
      case (_, ListValue(_)) => Ordering.GT
    }

  /** For testing only! */
  def orderPrimitive: Order[PrimitiveValue] =
    Order.order {
      case (BooleanValue(v1), BooleanValue(v2)) => v1 ?|? v2
      case (StringValue(v1), StringValue(v2)) => v1 ?|? v2
      case (IntValue(v1), IntValue(v2)) => v1 ?|? v2
      case (LongValue(v1), LongValue(v2)) => v1 ?|? v2
      case (DoubleValue(v1), DoubleValue(v2)) => v1 ?|? v2
      case (DateValue(v1), DateValue(v2)) => v1 ?|? v2
      case (BooleanValue(_), _) => Ordering.LT
      case (_, BooleanValue(_)) => Ordering.GT
      case (StringValue(_), _) => Ordering.LT
      case (_, StringValue(_)) => Ordering.GT
      case (IntValue(_), _) => Ordering.LT
      case (_, IntValue(_)) => Ordering.GT
      case (LongValue(_), _) => Ordering.LT
      case (_, LongValue(_)) => Ordering.GT
      case (DoubleValue(_), _) => Ordering.LT
      case (_, DoubleValue(_)) => Ordering.GT
      case (DateValue(_), _) => Ordering.LT
      case (_, DateValue(_)) => Ordering.GT
    }
}
