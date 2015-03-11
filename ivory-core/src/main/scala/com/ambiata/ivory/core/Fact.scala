package com.ambiata.ivory.core

import com.ambiata.ivory.core.thrift._
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

  val MaxEntityLength: Int = 256

  def newFactWithNamespace(entity: String, namespace: Namespace, feature: String, date: Date, time: Time, value: Value): Fact =
    newFact(entity, namespace.name, feature, date, time, value)

  def newFact(entity: String, namespace: String, feature: String, date: Date, time: Time, value: Value): Fact =
    FatThriftFact.factWith(entity, namespace, feature, date, time, Value.toThrift(value))

  /** For testing only! */
  def orderEntityDateTime: Order[Fact] =
    Order.orderBy[Fact, (String, Long)](f => f.entity -> f.datetime.long) |+|
      // We need to sort by value too for sets where the same entity/datetime exists
      Order.order { case (f1, f2) => Value.order(f1.value, f2.value) }

  def validate(fact: Fact, dict: Dictionary): Validation[String, Fact] =
    validateEntity(fact) match {
      case Success(f) =>
        dict.byFeatureId.get(f.featureId).map({
          case Concrete(_, fm) => Value.validateEncoding(f.value, fm.encoding).as(f).leftMap(_ + s" '${f.toString}'")
          case _: Virtual      => s"Cannot have virtual facts for ${f.featureId}".failure
        }).getOrElse(s"Dictionary entry '${f.featureId}' doesn't exist!".failure)
      case e @ Failure(_) =>
        e
    }

  def validateEntity(fact: Fact): Validation[String, Fact] =
    if(fact.entity.length > MaxEntityLength) Failure(s"Entity id '${fact.entity}' too large! Max length is ${MaxEntityLength}") else Success(fact)
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
