package com.ambiata.ivory.core

import com.ambiata.ivory.core.thrift._
import scala.collection.JavaConverters._
import scalaz._, Scalaz._

trait Fact {
  def entity: String
  def namespace(source: FeatureIdMappings): Namespace
  /** fast access method for the Fact Namespace which does not validate the namespace string */
  def namespaceUnsafe(source: FeatureIdMappings): Namespace
  def feature(source: FeatureIdMappings): String
  def featureId(source: FeatureIdMappings): FeatureId
  def featureIdIndex(source: FeatureIdMappings): FeatureIdIndex
  def date: Date
  def time: Time
  def value: Value

  def toThriftV1(source: FeatureIdMappings, target: FeatureIdMappings): ThriftFactV1
  def toThriftV2(source: FeatureIdMappings, target: FeatureIdMappings): ThriftFactV2

  def toNamespacedThriftV1(source: FeatureIdMappings, target: FeatureIdMappings): NamespacedFactV1
  def toNamespacedThriftV2(source: FeatureIdMappings, target: FeatureIdMappings): NamespacedFactV2

  def datetime: DateTime =
    date.addTime(time)

  def partition(source: FeatureIdMappings): Partition =
    Partition(namespace(source), date)

  def isTombstone: Boolean = value match {
    case TombstoneValue     => true
    case _                  => false
  }

  def withEntity(newEntity: String, source: FeatureIdMappings): Fact =
    Fact.newFactWithNamespace(newEntity, namespace(source), feature(source), date, time, value)

  def withFeatureId(newFeatureId: FeatureId): Fact =
    Fact.newFactWithNamespace(entity, newFeatureId.namespace, newFeatureId.name, date, time, value)

  def withDate(newDate: Date, source: FeatureIdMappings): Fact =
    Fact.newFactWithNamespace(entity, namespace(source), feature(source), newDate, time, value)

  def withTime(newTime: Time, source: FeatureIdMappings): Fact =
    Fact.newFactWithNamespace(entity, namespace(source), feature(source), date, newTime, value)

  def withValue(newValue: Value, source: FeatureIdMappings): Fact =
    Fact.newFactWithNamespace(entity, namespace(source), feature(source), date, time, newValue)
}

object Fact {
  def newFactWithNamespace(entity: String, namespace: Namespace, feature: String, date: Date, time: Time, value: Value): Fact =
    newFact(entity, namespace.name, feature, date, time, value)

  def newFact(entity: String, namespace: String, feature: String, date: Date, time: Time, value: Value): Fact =
    NamespacedThriftFactV1.factWith(entity, namespace, feature, date, time, Value.toThrift(value))
}

trait NamespacedThriftFactDerivedV1 extends Fact { self: NamespacedThriftFactV1  =>

  def entity: String =
    fact.getEntity

  def namespace(source: FeatureIdMappings): Namespace =
    // this name should be well-formed if the ThriftFact has been validated
    // if that's not the case an exception will be thrown here
    Namespace.reviewed(nspace)

  def namespaceUnsafe(source: FeatureIdMappings): Namespace =
    Namespace.unsafe(nspace)

  def feature(source: FeatureIdMappings): String =
    fact.attribute

  def featureId(source: FeatureIdMappings): FeatureId =
    FeatureId(namespace(source), fact.getAttribute)

  def featureIdIndex(source: FeatureIdMappings): FeatureIdIndex =
    source.byFeatureId.get(featureId(source)).get

  def date: Date =
    Date.unsafeFromInt(yyyyMMdd)

  def time: Time =
    Time.unsafe(seconds)

  def seconds: Int =
    fact.getSeconds

  def value: Value =
    Value.fromThrift(fact.getValue)

  def toThriftV1(source: FeatureIdMappings, target: FeatureIdMappings): ThriftFactV1 =
    fact

  def toThriftV2(source: FeatureIdMappings, target: FeatureIdMappings): ThriftFactV2 = {
    val tfact = new ThriftFactV2(entity, target.byFeatureId.get(featureId(source)).get.int, fact.getValue)
    if(seconds != 0) tfact.setSeconds(seconds) else tfact
  }

  def toNamespacedThriftV1(source: FeatureIdMappings, target: FeatureIdMappings): NamespacedFactV1 =
    this

  def toNamespacedThriftV2(source: FeatureIdMappings, target: FeatureIdMappings): NamespacedFactV2 =
    NamespacedThriftFactV2(entity, featureId(source), fact.getValue, date, time, target)

  // Overriding for performance to avoid extra an extra allocation
  override def isTombstone: Boolean =
    fact.getValue.isSetT
}

object NamespacedThriftFactV1 {
  def apply(ns: String, date: Date, tfact: ThriftFactV1): NamespacedFact =
    new NamespacedThriftFactV1(tfact, ns, date.int) with NamespacedThriftFactDerivedV1

  def factWith(entity: String, namespace: String, feature: String, date: Date, time: Time, value: ThriftFactValue): Fact = {
    val tfact = new ThriftFactV1(entity, feature, value)
    apply(namespace, date, if (time.seconds != 0) tfact.setSeconds(time.seconds) else tfact)
  }
}

trait NamespacedThriftFactDerivedV2 extends Fact { self: NamespacedThriftFactV2  =>

  def entity: String =
    getEnty

  def namespace(source: FeatureIdMappings): Namespace =
    featureId(source).namespace

  def namespaceUnsafe(source: FeatureIdMappings): Namespace =
    featureId(source).namespace

  def feature(source: FeatureIdMappings): String =
    featureId(source).name

  def featureId(source: FeatureIdMappings): FeatureId =
    source.getUnsafe(featureIdIndex(source))

  def featureIdIndex(source: FeatureIdMappings): FeatureIdIndex =
    FeatureIdIndex(findex)

  def date: Date =
    Date.unsafeFromInt(yyyyMMdd)

  def time: Time =
    Time.unsafe(seconds)

  def seconds: Int =
    getSecs

  def value: Value =
    Value.fromThrift(getTvalue)

  def toThriftV1(source: FeatureIdMappings, target: FeatureIdMappings): ThriftFactV1 = {
    val tfact = new ThriftFactV1(entity, feature(source), getTvalue)
    if(seconds != 0) tfact.setSeconds(seconds) else tfact
  }

  def toThriftV2(source: FeatureIdMappings, target: FeatureIdMappings): ThriftFactV2 = {
    val tfact = new ThriftFactV2(entity, target.byFeatureId.get(featureId(source)).get.int, getTvalue)
    if(seconds != 0) tfact.setSeconds(seconds) else tfact
  }

  def toNamespacedThriftV1(source: FeatureIdMappings, target: FeatureIdMappings): NamespacedFactV1 =
    NamespacedThriftFactV1(namespace(source).name, date, toThriftV1(source, target))

  def toNamespacedThriftV2(source: FeatureIdMappings, target: FeatureIdMappings): NamespacedFactV2 = {
    this.findex = target.byFeatureId.get(featureId(source)).get.int
    this
  }

  // Overriding for performance to avoid extra an extra allocation
  override def isTombstone: Boolean =
    getTvalue.isSetT
}

object NamespacedThriftFactV2 {
  def apply(entity: String, featureId: FeatureId, value: ThriftFactValue, date: Date, time: Time, mappings: FeatureIdMappings): NamespacedFactV2 = {
    val index: FeatureIdIndex = mappings.byFeatureId.get(featureId).get
    val tfact = new NamespacedThriftFactV2(entity, index.int, value, date.int) with NamespacedThriftFactDerivedV2
    if (time.seconds != 0) tfact.setSecs(time.seconds)
    tfact
  }
}

object BooleanFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Boolean): Fact =
    NamespacedThriftFactV1.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.b(value))

  val fromTuple = apply _ tupled
}

object IntFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Int): Fact =
    NamespacedThriftFactV1.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.i(value))

  val fromTuple = apply _ tupled
}

object LongFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Long): Fact =
    NamespacedThriftFactV1.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.l(value))

  val fromTuple = apply _ tupled
}

object DoubleFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Double): Fact =
    NamespacedThriftFactV1.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.d(value))

  val fromTuple = apply _ tupled
}

object StringFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: String): Fact =
    NamespacedThriftFactV1.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.s(value))

  val fromTuple = apply _ tupled
}

object DateFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time, value: Date): Fact =
    NamespacedThriftFactV1.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.date(value.int))
}

object TombstoneFact {
  def apply(entity: String, featureId: FeatureId, date: Date, time: Time): Fact =
    NamespacedThriftFactV1.factWith(entity, featureId.namespace.name, featureId.name, date, time, ThriftFactValue.t(new ThriftTombstone()))

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

  def validateFact(fact: Fact, dict: Dictionary, source: FeatureIdMappings): Validation[String, Fact] =
    validateEntity(fact) match {
      case Success(f) =>
        val featureId = f.featureId(source)
        dict.byFeatureId.get(featureId).map({
          case Concrete(_, fm) => validateEncoding(f.value, fm.encoding).as(f).leftMap(_ + s" '${f.toString}'")
          case _: Virtual      => s"Cannot have virtual facts for ${featureId}".failure
        }).getOrElse(s"Dictionary entry '${featureId}' doesn't exist!".failure)
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

  def toThrift(value: Value): ThriftFactValue = {
    def primValue(p: PrimitiveValue): ThriftFactPrimitiveValue = p match {
      // This duplication here is annoying/unfortunate - and will require a backwards-incompatible change
      case StringValue(s)   => ThriftFactPrimitiveValue.s(s)
      case BooleanValue(b)  => ThriftFactPrimitiveValue.b(b)
      case IntValue(i)      => ThriftFactPrimitiveValue.i(i)
      case LongValue(l)     => ThriftFactPrimitiveValue.l(l)
      case DoubleValue(d)   => ThriftFactPrimitiveValue.d(d)
      case DateValue(r)     => ThriftFactPrimitiveValue.date(r.int)
    }
    value match {
      case StringValue(s)   => ThriftFactValue.s(s)
      case BooleanValue(b)  => ThriftFactValue.b(b)
      case IntValue(i)      => ThriftFactValue.i(i)
      case LongValue(l)     => ThriftFactValue.l(l)
      case DoubleValue(d)   => ThriftFactValue.d(d)
      case TombstoneValue   => ThriftFactValue.t(new ThriftTombstone())
      case DateValue(r)     => ThriftFactValue.date(r.int)
      case ListValue(v)     => ThriftFactValue.lst(new ThriftFactList(v.map {
        case p: PrimitiveValue  => ThriftFactListValue.p(primValue(p))
        case StructValue(m)     => ThriftFactListValue.s(new ThriftFactStructSparse(m.mapValues(primValue).asJava))
      }.asJava))
      case StructValue(m)   => ThriftFactValue.structSparse(new ThriftFactStructSparse(m.mapValues(primValue).asJava))
    }
  }

  def fromThrift(value: ThriftFactValue): Value = {
    def factPrimitiveToValue(v: ThriftFactPrimitiveValue): PrimitiveValue = v match {
      case tsv if tsv.isSetD => DoubleValue(tsv.getD)
      case tsv if tsv.isSetS => StringValue(tsv.getS)
      case tsv if tsv.isSetI => IntValue(tsv.getI)
      case tsv if tsv.isSetL => LongValue(tsv.getL)
      case tsv if tsv.isSetB => BooleanValue(tsv.getB)
      case tsv if tsv.isSetDate => DateValue(Date.unsafeFromInt(tsv.getDate))
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    value match {
      case tv if tv.isSetD => DoubleValue(tv.getD)
      case tv if tv.isSetS => StringValue(tv.getS)
      case tv if tv.isSetI => IntValue(tv.getI)
      case tv if tv.isSetL => LongValue(tv.getL)
      case tv if tv.isSetB => BooleanValue(tv.getB)
      case tv if tv.isSetDate => DateValue(Date.unsafeFromInt(tv.getDate))
      case tv if tv.isSetT => TombstoneValue
      case tv if tv.isSetStructSparse
                           => StructValue(tv.getStructSparse.getV.asScala.toMap.mapValues(factPrimitiveToValue))
      case tv if tv.isSetLst
                           => ListValue(tv.getLst.getL.asScala.map {
        case l if l.isSetP => factPrimitiveToValue(l.getP)
        case l if l.isSetS => StructValue(l.getS.getV.asScala.toMap.mapValues(factPrimitiveToValue))
      }.toList)
      case _               => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${value.toString}].'")
    }
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
}
