package com.ambiata.ivory.operation.ingestion.thrift

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.MutableList
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.{thrift => ct}
import org.joda.time.DateTimeZone
import scala.collection.JavaConverters._
import scalaz.{Name =>_,_}, Scalaz._

object Conversion {

  def thrift2fact(namespace: String, thrift: ThriftFact, local: DateTimeZone, ivory: DateTimeZone): \/[String, Fact] =
    for {
      date  <- Dates.parse(thrift.datetime, local, ivory).toRightDisjunction(s"Thrift date was invalid: '${thrift.datetime}'")
      value <- thrift2value(thrift.value)
    } yield FatThriftFact.factWith(thrift.entity, namespace, thrift.attribute, date.fold(identity, _.date), date.fold(_ => Time(0), _.time), value)

  def thrift2value(fact: ThriftFactValue): \/[String, ct.ThriftFactValue] = {
    // Sigh
    def convertPrim1(v: ThriftFactPrimitiveValue): \/[String, ct.ThriftFactValue] = v match {
      case tsv if tsv.isSetD => \/-(ct.ThriftFactValue.d(tsv.getD))
      case tsv if tsv.isSetS => \/-(ct.ThriftFactValue.s(tsv.getS))
      case tsv if tsv.isSetI => \/-(ct.ThriftFactValue.i(tsv.getI))
      case tsv if tsv.isSetL => \/-(ct.ThriftFactValue.l(tsv.getL))
      case tsv if tsv.isSetB => \/-(ct.ThriftFactValue.b(tsv.getB))
      case tsv if tsv.isSetDate => Dates.date(tsv.getDate).toRightDisjunction(s"Thrift date (as a value) was invalid: ${tsv.getDate}").map(v => ct.ThriftFactValue.date(v.int))
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    def convertPrim(v: ThriftFactPrimitiveValue): \/[String, ct.ThriftFactPrimitiveValue] = v match {
      case tsv if tsv.isSetD => \/-(ct.ThriftFactPrimitiveValue.d(tsv.getD))
      case tsv if tsv.isSetS => \/-(ct.ThriftFactPrimitiveValue.s(tsv.getS))
      case tsv if tsv.isSetI => \/-(ct.ThriftFactPrimitiveValue.i(tsv.getI))
      case tsv if tsv.isSetL => \/-(ct.ThriftFactPrimitiveValue.l(tsv.getL))
      case tsv if tsv.isSetB => \/-(ct.ThriftFactPrimitiveValue.b(tsv.getB))
      case tsv if tsv.isSetDate => Dates.date(tsv.getDate).toRightDisjunction(s"Thrift date (as a value) was invalid: ${tsv.getDate}").map(v => ct.ThriftFactPrimitiveValue.date(v.int))
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    def traverseMap(m: MMap[String, ThriftFactPrimitiveValue]): \/[String, Map[String, ct.ThriftFactPrimitiveValue]] = {
      val it = m.iterator
      var theMap = MMap[String, ct.ThriftFactPrimitiveValue]()
      while (it.hasNext) {
        val (s, pv) = it.next
        convertPrim(pv) match {
          case -\/(m) => return -\/(m)
          case \/-(v) => theMap += ((s, v))
        }
      }
      return \/-(theMap.toMap)
    }
    def traverseLst(m: List[ThriftFactListValue]): \/[String, List[ct.ThriftFactListValue]] = {
      val it = m.iterator
      var theLst = MutableList[ct.ThriftFactListValue]()
      while (it.hasNext) {
        val dis: \/[String, ct.ThriftFactListValue] = it.next match {
          case lv if lv.isSetP     => convertPrim(lv.getP).map(v => ct.ThriftFactListValue.p(v))
          case lv if lv.isSetS     => traverseMap(lv.getS.getV.asScala).map(m => new ct.ThriftFactStructSparse(m.asJava)).map(v => ct.ThriftFactListValue.s(v))
        }
        dis match {
          case -\/(m) => return -\/(m)
          case \/-(v) => theLst += v
        }
      }
      return \/-(theLst.toList)
    }
    fact match {
      case x if x.isSetTombstone => \/-(ct.ThriftFactValue.t(new ct.ThriftTombstone()))
      case x if x.isSetPrimitive => convertPrim1(x.getPrimitive)
      case x if x.isSetStrct     => traverseMap(x.getStrct.getV.asScala).map(m => ct.ThriftFactValue.structSparse(new ct.ThriftFactStructSparse(m.asJava)))
      case x if x.isSetLst       => traverseLst(x.getLst.getL.asScala.toList).map(m => ct.ThriftFactValue.lst(new ct.ThriftFactList(m.asJava)))
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
      case tsv if tsv.isSetDate => ThriftFactPrimitiveValue.date(Date.unsafeFromInt(tsv.getDate).hyphenated)
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    def convertPrim(v: ct.ThriftFactPrimitiveValue): ThriftFactPrimitiveValue = v match {
      case tsv if tsv.isSetD => ThriftFactPrimitiveValue.d(tsv.getD)
      case tsv if tsv.isSetS => ThriftFactPrimitiveValue.s(tsv.getS)
      case tsv if tsv.isSetI => ThriftFactPrimitiveValue.i(tsv.getI)
      case tsv if tsv.isSetL => ThriftFactPrimitiveValue.l(tsv.getL)
      case tsv if tsv.isSetB => ThriftFactPrimitiveValue.b(tsv.getB)
      case tsv if tsv.isSetDate => ThriftFactPrimitiveValue.date(Date.unsafeFromInt(tsv.getDate).hyphenated)
      case _                 => Crash.error(Crash.CodeGeneration, s"You have hit a code generation issue. This is a BUG. Do not continue, code needs to be updated to handle new thrift structure. [${v.toString}].'")
    }
    value match {
      case x if x.isSetT            => ThriftFactValue.tombstone(new ThriftTombstone())
      case x if x.isSetStructSparse => ThriftFactValue.strct(new ThriftFactStruct(x.getStructSparse.getV.asScala.mapValues(convertPrim).asJava))
      case x if x.isSetLst          => ThriftFactValue.lst(new ThriftFactList(x.getLst.getL.asScala.map {
        case lv if lv.isSetP        => ThriftFactListValue.p(convertPrim(lv.getP))
        case lv if lv.isSetS        => ThriftFactListValue.s(new ThriftFactStruct(lv.getS.getV.asScala.mapValues(convertPrim).asJava))
      }.asJava))
      case _                        => ThriftFactValue.primitive(convertPrim1(value))
    }
  }
}
