package com.ambiata.ivory.storage.legacy

import scalaz.{Name => _, DList => _, Value => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDateTime
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi._, FactFormats._
import com.ambiata.ivory.storage.parse.EavtParsers

object DelimitedFactTextStorage {
  case class DelimitedFactTextLoader(path: String, dict: Dictionary) extends IvoryScoobiLoader[Fact] {
    def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ Fact] = {
      fromTextFile(path).map(line => parseFact(dict, line))
    }
  }

  case class DelimitedFactTextStorer(path: Path, delim: String = "|", tombstoneValue: Option[String] = Some("☠")) extends IvoryScoobiStorer[Fact, DList[String]] {
    def storeScoobi(dlist: DList[Fact])(implicit sc: ScoobiConfiguration): DList[String] =
    dlist.mapFlatten(f =>
      Value.toString(f.value, tombstoneValue).map(v => f.entity + delim + f.namespace + ":" + f.featureId.name + delim + v + delim + time(f.date, f.time.seconds).toString("yyyy-MM-dd HH:mm:ss"))
    ).toTextFile(path.toString, overwrite = true)

    def time(d: Date, s: Int): LocalDateTime =
      d.localDate.toDateTimeAtStartOfDay.toLocalDateTime.plusSeconds(s)
  }

  def parseFact(dict: Dictionary, str: String): ParseError \/ Fact =
    factParser(dict).run(str.split('|').toList).leftMap(ParseError.withLine(str)).disjunction

  def factParser(dict: Dictionary): ListParser[Fact] = {
    import ListParser._
    for {
      entity <- string.nonempty
      attr   <- string.nonempty
      fid    <- value(featureIdParser.run(attr.split(":", 2).toList))
      rawv   <- string
      v      <- value(EavtParsers.validateFeature(dict, fid, rawv))
      date   <- localDatetime("yyyy-MM-dd HH:mm:ss") // TODO replace with something that doesn't convert to joda
    } yield Fact.newFact(entity, fid.namespace.name, fid.name, Date.fromLocalDate(date.toLocalDate), Time.unsafe(date.getMillisOfDay / 1000), v)
  }

  def featureIdParser: ListParser[FeatureId] = {
    import ListParser._
    for {
      ns   <- Name.listParser
      name <- string.nonempty
    } yield FeatureId(ns, name)
  }
}
