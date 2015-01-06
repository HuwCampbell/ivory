package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift._

trait ReductionValueFrom[@specialized(Int, Float, Double, Boolean) A] {
  def from(a: ThriftFactValue): A
}

trait ReductionValueFromPrim[@specialized(Int, Float, Double, Boolean) A] {
  def fromPrim(a: ThriftFactPrimitiveValue): A
}

trait ReductionValueTo[@specialized(Int, Float, Double, Boolean) A] {
  def to(a: A, value: ThriftFactValue): Unit
}

trait ReductionValueToPrim[@specialized(Int, Float, Double, Boolean) A] {
  def toPrim(a: A): ThriftFactPrimitiveValue
}

object ReductionValueString extends ReductionValueFrom[String] with ReductionValueFromPrim[String] with ReductionValueTo[String] with ReductionValueToPrim[String] {
  def from(v: ThriftFactValue): String = v.getS
  def fromPrim(v: ThriftFactPrimitiveValue): String = v.getS
  def to(v: String, t: ThriftFactValue) = t.setS(v)
  def toPrim(v: String): ThriftFactPrimitiveValue = ThriftFactPrimitiveValue.s(v)
}

object ReductionValueBoolean extends ReductionValueFrom[Boolean] with ReductionValueFromPrim[Boolean] with ReductionValueTo[Boolean] with ReductionValueToPrim[Boolean] {
  def from(v: ThriftFactValue): Boolean = v.getB
  def fromPrim(v: ThriftFactPrimitiveValue): Boolean = v.getB
  def to(v: Boolean, t: ThriftFactValue): Unit = t.setB(v)
  def toPrim(v: Boolean): ThriftFactPrimitiveValue = ThriftFactPrimitiveValue.b(v)
}

object ReductionValueInt extends ReductionValueFrom[Int] with ReductionValueFromPrim[Int] with ReductionValueTo[Int] with ReductionValueToPrim[Int] {
  def from(v: ThriftFactValue): Int = v.getI
  def fromPrim(v: ThriftFactPrimitiveValue): Int = v.getI
  def to(v: Int, t: ThriftFactValue): Unit = t.setI(v)
  def toPrim(v: Int): ThriftFactPrimitiveValue = ThriftFactPrimitiveValue.i(v)
}

object ReductionValueLong extends ReductionValueFrom[Long] with ReductionValueFromPrim[Long] with ReductionValueTo[Long] with ReductionValueToPrim[Long] {
  def from(v: ThriftFactValue): Long = v.getL
  def fromPrim(v: ThriftFactPrimitiveValue): Long = v.getL
  def to(v: Long, t: ThriftFactValue): Unit = t.setL(v)
  def toPrim(v: Long): ThriftFactPrimitiveValue = ThriftFactPrimitiveValue.l(v)
}

object ReductionValueDouble extends ReductionValueFrom[Double] with ReductionValueFromPrim[Double] with ReductionValueTo[Double] with ReductionValueToPrim[Double] {
  def from(v: ThriftFactValue): Double = v.getD
  def fromPrim(v: ThriftFactPrimitiveValue): Double = v.getD
  def to(d: Double, t: ThriftFactValue): Unit = t.setD(d)
  def toPrim(v: Double): ThriftFactPrimitiveValue = ThriftFactPrimitiveValue.d(v)
}

object ReductionValueDate extends ReductionValueFrom[Int] with ReductionValueFromPrim[Int] with ReductionValueTo[Int] with ReductionValueToPrim[Int] {
  def from(v: ThriftFactValue): Int = v.getDate
  def fromPrim(v: ThriftFactPrimitiveValue): Int = v.getDate
  def to(v: Int, t: ThriftFactValue) = t.setDate(v)
  def toPrim(v: Int): ThriftFactPrimitiveValue = ThriftFactPrimitiveValue.date(v)
}

case class ValueOrTombstone[@specialized(Int, Float, Double, Boolean) A](var value: A, var tombstone: Boolean)

class ReductionValueOrTombstone[@specialized(Int, Float, Double, Boolean) A](toValue: ReductionValueTo[A]) extends ReductionValueTo[ValueOrTombstone[A]] {
  val tombstone = new ThriftTombstone
  def to(v: ValueOrTombstone[A], t: ThriftFactValue) =
  if (v.tombstone) t.setT(tombstone)
  else toValue.to(v.value, t)
}

case class ReductionValueStruct[K, V](TO: ReductionValueToPrim[V]) extends ReductionValueTo[KeyValue[K, V]] {
  import scala.collection.JavaConverters._
  def to(d: KeyValue[K, V], value: ThriftFactValue) =
    // NOTE: We _have_ to convert to a String key at this point
    value.setStructSparse(new ThriftFactStructSparse(d.map.asScala.map { case (k, v) => k.toString -> TO.toPrim(v)}.asJava))
}

case class ReductionValueList[A](TO: ReductionValueToPrim[A]) extends ReductionValueTo[List[A]] {
  import scala.collection.JavaConverters._
  def to(d: List[A], value: ThriftFactValue) =
    value.setLst(new ThriftFactList(d.map { case a => ThriftFactListValue.p(TO.toPrim(a))}.asJava))
}
