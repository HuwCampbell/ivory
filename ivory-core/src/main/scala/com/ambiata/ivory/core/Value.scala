package com.ambiata.ivory.core

import com.ambiata.ivory.core.thrift._
import scala.collection.JavaConverters._
import scalaz._, Scalaz._


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

  object text {

    def toString(v: Value, tombstoneValue: Option[String]): Option[String] = v match {
      case TombstoneValue    => tombstoneValue
      case p: PrimitiveValue => Some(Value.toStringPrimitive(p))
      // Currently we're ignoring lists/structs in all text format (for now)
      case _: ListValue      => None
      case StructValue(_)    => None
    }

    def toStringOr(v: Value, tombstoneValue: String): Option[String] =
      toString(v, Some(tombstoneValue))

    /** This is _not_ for general consumption - should only be use for testing or diffing */
    def toStringWithStruct(v: Value, tombstone: String): String = v match {
      case TombstoneValue      => tombstone
      case p: PrimitiveValue   => Value.toStringPrimitive(p)
      case ListValue(values)   => "[" + values.map(toStringWithStruct(_, tombstone)).mkString(",") + "]"
      case StructValue(values) =>
        "(" + values.map { case (k, p) => k + ":" + Value.toStringPrimitive(p)}.mkString(",") + ")"
    }
  }

  object json {

    import argonaut._, Argonaut._

    def toStringWithStruct(v: Value, tombstone: String): String = v match {
      case TombstoneValue      => tombstone
      case p: PrimitiveValue   => Value.toStringPrimitive(p)
      case l: ListValue        => toJson(l).nospaces
      case s: StructValue      => toJson(s).nospaces
    }

    def toJson(v: Value): Json = v match {
      case TombstoneValue   => jNull
      case BooleanValue(b)  => jBool(b)
      case IntValue(i)      => jNumberOrNull(i)
      case LongValue(l)     => jNumberOrNull(l)
      case DoubleValue(d)   => jNumberOrNull(d)
      case DateValue(r)     => jString(r.hyphenated)
      case StringValue(s)   => jString(s)
      case StructValue(xs)  => jObject(xs.foldLeft(JsonObject.empty){ case(jo, (key, value)) => jo.+:(key -> toJson(value)) })
      case ListValue(ls)    => jArray(ls.map(toJson))
    }

    def decodeJson(encoding: Encoding, raw: String): String \/ Value =
      raw.decodeOption[Json].toRightDisjunction(s"Value '$raw' is not valid JSON").flatMap { json: Json =>
        if (json.isNull)
          TombstoneValue.right
        else
          encoding.fold(
            p => parseJsonPrimitive(p, json),
            s => parseJsonStruct(s, json),
            l => parseJsonList(l, json)
          )
      }

    def parseJsonStruct(encoding: StructEncoding, json: Json): String \/ StructValue = for {
      jm <- json.obj.map(_.toMap).toRightDisjunction(s"Value '$json' is not an object")
      ps <- encoding.values.toList.traverseU { case (key, StructEncodedValue(enc, optional)) =>
        if (optional) {
          jm.get(key).traverseU(x => parseJsonPrimitive(enc, x).map(v => key -> v))
        } else {
          jm.get(key).toRightDisjunction(s"Mandatory field '$key' not found in '$json'")
            .flatMap(x => parseJsonPrimitive(enc, x).map(v => (key -> v).some))
        }
      }.map(_.flatten)
      _  <- if (jm.size == ps.length) \/-(()) else {
          val leftOverKeys = (jm.keys.toSet -- ps.map(_._1).toSet)
          -\/(s"Unsupported fields: '${leftOverKeys.mkString("' '")}' in '${json}'")
        }
    } yield StructValue(ps.toMap)

    def parseJsonList(encoding: ListEncoding, json: Json): String \/ ListValue = for {
      jl <- json.array.toRightDisjunction(s"Value '$json' is not an array")
      ls <- encoding.encoding.fold(
        pe => jl.traverseU(v => parseJsonPrimitive(pe, v)),
        se => jl.traverseU(v => parseJsonStruct(se, v))
      )
    } yield ListValue(ls)

    def parseJsonPrimitive(encoding: PrimitiveEncoding, json: Json): String \/ PrimitiveValue = encoding match {
      case BooleanEncoding   => json.bool.toRightDisjunction(s"Value '$json' is not a boolean").map(v => BooleanValue(v))
      case IntEncoding       => json.number.flatMap(_.toInt).toRightDisjunction(s"Value '$json' is not an integer").map(v => IntValue(v))
      case LongEncoding      => json.number.flatMap(_.toLong).toRightDisjunction(s"Value '$json' is not a long").map(v => LongValue(v))
      case DoubleEncoding    => json.number.map(_.toDouble).toRightDisjunction(s"Value '$json' is not a double").map(v => DoubleValue(v))
      case DateEncoding      => json.string.flatMap(Dates.date).toRightDisjunction(s"Value '$json' is not a date").map(v => DateValue(v))
      case StringEncoding    => json.string.toRightDisjunction(s"Value '$json' is not a string").map(v => StringValue(v))
    }
  }
}
