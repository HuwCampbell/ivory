package com.ambiata.ivory.operation.ingestion.thrift

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.{thrift => ct}
import org.joda.time.DateTimeZone
import scala.collection.JavaConverters._
import scalaz.{Name =>_,_}, Scalaz._

object Conversion {

  def thrift2fact(namespace: String, thrift: ThriftFact, local: DateTimeZone, ivory: DateTimeZone): \/[String, Fact] =
    for {
      _    <- Option(thrift.value).toRightDisjunction("Fact could not be parsed")
      date <- Dates.parse(thrift.datetime, local, ivory).toRightDisjunction("Thrift date was invalid")
    } yield FatThriftFact.factWith(thrift.entity, namespace, thrift.attribute, date.fold(identity, _.date), date.fold(_ => Time(0), _.time), thrift2value(thrift.value))

  def thrift2value(fact: ThriftFactValue): ct.ThriftFactValue = {
    // Sigh
    def convertPrim1(v: ThriftFactPrimitiveValue): ct.ThriftFactValue = v match {
      case tsv if tsv.isSetD => ct.ThriftFactValue.d(tsv.getD)
      case tsv if tsv.isSetS => ct.ThriftFactValue.s(tsv.getS)
      case tsv if tsv.isSetI => ct.ThriftFactValue.i(tsv.getI)
      case tsv if tsv.isSetL => ct.ThriftFactValue.l(tsv.getL)
      case tsv if tsv.isSetB => ct.ThriftFactValue.b(tsv.getB)
      case tsv if tsv.isSetT => ct.ThriftFactValue.t(new ct.ThriftTombstone())
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    def convertPrim(v: ThriftFactPrimitiveValue): ct.ThriftFactPrimitiveValue = v match {
      case tsv if tsv.isSetD => ct.ThriftFactPrimitiveValue.d(tsv.getD)
      case tsv if tsv.isSetS => ct.ThriftFactPrimitiveValue.s(tsv.getS)
      case tsv if tsv.isSetI => ct.ThriftFactPrimitiveValue.i(tsv.getI)
      case tsv if tsv.isSetL => ct.ThriftFactPrimitiveValue.l(tsv.getL)
      case tsv if tsv.isSetB => ct.ThriftFactPrimitiveValue.b(tsv.getB)
      case tsv if tsv.isSetT => ct.ThriftFactPrimitiveValue.t(new ct.ThriftTombstone())
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    fact match {
      case x if x.isSetPrimitive => convertPrim1(x.getPrimitive)
      case x if x.isSetStrct     => ct.ThriftFactValue.structSparse(new ct.ThriftFactStructSparse(x.getStrct.getV.asScala.mapValues(convertPrim).asJava))
      case x if x.isSetLst       => ct.ThriftFactValue.lst(new ct.ThriftFactList(x.getLst.getL.asScala.map {
        case lv if lv.isSetP     => ct.ThriftFactListValue.p(convertPrim(lv.getP))
        case lv if lv.isSetS     => ct.ThriftFactListValue.s(new ct.ThriftFactStructSparse(lv.getS.getV.asScala.mapValues(convertPrim).asJava))
      }.asJava))
    }
  }

  // This is only for testing but it doesn't hurt to keep both sides of the conversion together
  def fact2thrift(fact: Fact): ThriftFact =
    new ThriftFact(fact.entity, fact.feature, value2thrift(fact.toThrift.value), fact.date.string("-") + 'T' + fact.time.hhmmss)

  def value2thrift(value: ct.ThriftFactValue): ThriftFactValue = {
    // Sigh
    def convertPrim1(v: ct.ThriftFactValue): ThriftFactPrimitiveValue = v match {
      case tsv if tsv.isSetD => ThriftFactPrimitiveValue.d(tsv.getD)
      case tsv if tsv.isSetS => ThriftFactPrimitiveValue.s(tsv.getS)
      case tsv if tsv.isSetI => ThriftFactPrimitiveValue.i(tsv.getI)
      case tsv if tsv.isSetL => ThriftFactPrimitiveValue.l(tsv.getL)
      case tsv if tsv.isSetB => ThriftFactPrimitiveValue.b(tsv.getB)
      case tsv if tsv.isSetT => ThriftFactPrimitiveValue.t(new ThriftTombstone())
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    def convertPrim(v: ct.ThriftFactPrimitiveValue): ThriftFactPrimitiveValue = v match {
      case tsv if tsv.isSetD => ThriftFactPrimitiveValue.d(tsv.getD)
      case tsv if tsv.isSetS => ThriftFactPrimitiveValue.s(tsv.getS)
      case tsv if tsv.isSetI => ThriftFactPrimitiveValue.i(tsv.getI)
      case tsv if tsv.isSetL => ThriftFactPrimitiveValue.l(tsv.getL)
      case tsv if tsv.isSetB => ThriftFactPrimitiveValue.b(tsv.getB)
      case tsv if tsv.isSetT => ThriftFactPrimitiveValue.t(new ThriftTombstone())
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    value match {
      case x if x.isSetStructSparse => ThriftFactValue.strct(new ThriftFactStruct(x.getStructSparse.getV.asScala.mapValues(convertPrim).asJava))
      case x if x.isSetLst          => ThriftFactValue.lst(new ThriftFactList(x.getLst.getL.asScala.map {
        case lv if lv.isSetP        => ThriftFactListValue.p(convertPrim(lv.getP))
        case lv if lv.isSetS        => ThriftFactListValue.s(new ThriftFactStruct(lv.getS.getV.asScala.mapValues(convertPrim).asJava))
      }.asJava))
      case _                        => ThriftFactValue.primitive(convertPrim1(value))
    }
  }
}
