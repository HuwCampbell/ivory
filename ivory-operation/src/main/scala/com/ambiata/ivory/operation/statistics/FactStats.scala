package com.ambiata.ivory.operation.statistics

import java.io.{DataInput, DataOutput}

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import com.ambiata.notion.core._
import FactFormats._

import spire.math._
import spire.implicits._

import argonaut._, Argonaut._

import scala.collection.mutable.{Map => MMap}

object FactStats {
  type KeyInfo        = (String, Date)
  type Histogram      = Map[String,Int]
  type NumericalStats = (Long, Double, Double)
  type FactStatEncode = (KeyInfo, Either[NumericalStats, Histogram])

  def statisticsForFactSet(repo: HdfsRepository, input: FactsetId): ScoobiAction[Unit] = for {
    dlist <- PartitionFactThriftStorageV2.loadScoobiWith(repo, input, None, None).flatMap(parseError)
    _     <- scoobiJob(dlist, repo.toIvoryLocation((Repository.factset(input) / "_stats")))
  } yield ()

  def parseError(dlist: DList[ParseError \/ Fact]): ScoobiAction[DList[Fact]] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      dlist.map({
        case -\/(e) => Crash.error(Crash.DataIntegrity, s"Can not parse fact - ${e}")
        case \/-(f) => f
      })
    })

  def scoobiJob(facts: DList[Fact], outputPath: IvoryLocation): ScoobiAction[Unit] = {
    ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>

      val grp = facts.groupBy({ case fact => (fact.featureId, fact.date)})
      val stats = grp.map({ case (key, vs) => genStats(key, vs)}).flatten
      val jsonstats = stats.map(_.asJson.nospaces)

      persist(jsonstats.toTextFile(outputPath.show, overwrite = false))
      ()
    }
  }

  implicit val DateWireFormat = new WireFormat[Date] {
    val p = implicitly[WireFormat[Int]]
    def toWire(x: Date, out: DataOutput) = p.toWire(x.int, out)
    def fromWire(in: DataInput) = Date.unsafeFromInt(p.fromWire(in))
  }

  def genStats(key: (FeatureId, Date), vs: Iterable[Fact]): List[FactStatEncode] = {

    var count: Long   = 0L
    var sum: Double   = 0.0
    var sqsum: Double = 0.0
    val histogram = MMap[String, Int]()

    def addNumeric[A: Numeric](d: A) {
      count += 1
      sum += d.toDouble
      sqsum += d.toDouble * d.toDouble
      addToHistogram(d.toString)
    }

    def addToHistogram(s: String) {
      if (histogram.size < 100) {
        histogram.get(s) match {
          case Some(i) => histogram(s) = i + 1
          case None    => histogram(s) = 1
        }
      }
    }

    vs.map(_.value).foreach(_ match {
      case IntValue(i)      => addNumeric(i)
      case LongValue(l)     => addNumeric(l)
      case DoubleValue(d)   => addNumeric(d)
      case TombstoneValue   => addToHistogram("☠")
      case StringValue(s)   => addToHistogram(s)
      case BooleanValue(b)  => addToHistogram(b.toString)
      case DateValue(r)     => addToHistogram(r.hyphenated)
      case ListValue(v)     => addToHistogram("List entries")
      case StructValue(m)   => addToHistogram("Struct entries")
    })

    List[FactStatEncode]() ++ 
    (if (histogram.size < 100)
      List(((key._1.toString, key._2), Right(histogram.toMap)))
     else Nil ) ++ (if (count > 0) {
      val mean = sum / count
      List(((key._1.toString, key._2), Left((count, mean, Math.sqrt(sqsum / count - mean * mean)))))
     } else Nil ) 
  }

  // Note: On merge with snapshot meta branch, these should be deleted
  implicit def DateJsonCodec: CodecJson[Date] = CodecJson.derived(
    EncodeJson(_.int.asJson),
    DecodeJson.optionDecoder(_.as[Int].toOption.flatMap(Date.fromInt), "Date"))
}
