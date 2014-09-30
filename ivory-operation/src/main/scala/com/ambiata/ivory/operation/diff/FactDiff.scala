package com.ambiata.ivory.operation.diff

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, Value => _, _}, Scalaz._
import com.ambiata.poacher.scoobi._
import com.ambiata.mundane.io._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.scoobi._
import FactFormats._

object FactDiff {

  def partitionFacts(input1: IvoryLocation, input2: IvoryLocation, outputPath: IvoryLocation): ScoobiAction[Unit] = {
    for {
      dlist1 <- PartitionFactThriftStorageV2.loadScoobiFromPaths(List(input1.path </> "*" </> "*" </> "*" </> "*").map(_.path)).flatMap(parseError)
      dlist2 <- PartitionFactThriftStorageV2.loadScoobiFromPaths(List(input2.path </> "*" </> "*" </> "*" </> "*").map(_.path)).flatMap(parseError)
      _      <- scoobiJob(dlist1, dlist2, outputPath)
    } yield ()
  }

  def parseError(dlist: DList[ParseError \/ Fact]): ScoobiAction[DList[Fact]] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      dlist.map({
        case -\/(e) => Crash.error(Crash.DataIntegrity, s"Can not parse fact - ${e}")
        case \/-(f) => f
      })
    })

  def flatFacts(input1: IvoryLocation, input2: IvoryLocation, outputPath: IvoryLocation): ScoobiAction[Unit] = for {
    res <- ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
             val dlist1 = valueFromSequenceFile[Fact](input1.path.path)
             val dlist2 = valueFromSequenceFile[Fact](input2.path.path)
             (dlist1, dlist2)
           })
    (dlist1, dlist2) = res
    _   <- scoobiJob(dlist1, dlist2, outputPath)
  } yield ()

  def scoobiJob(first_facts: DList[Fact], second_facts: DList[Fact], outputPath: IvoryLocation): ScoobiAction[Unit] = {
    ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>

      val facts = first_facts.map((true, _)) ++ second_facts.map((false, _))

      val grp = facts.groupBy({ case (flag, fact) => (fact.entity, fact.featureId.toString, fact.date.int, fact.time.seconds, Value.toStringWithStruct(fact.value)) })

      val diff: DList[List[(Boolean, Fact)]] = grp.mapFlatten({ case (_, vs) =>
        vs.toList match {
          case (true, f1) :: (false, f2) :: Nil => None
          case (false, f2) :: (true, f1) :: Nil => None
          case other                            => Some(other)
        }
      })

      val out: DList[String] = diff.map({
        case (true, fact) :: Nil  => s"Fact '${fact}' does not exist in input2"
        case (false, fact) :: Nil => s"Fact '${fact}' does not exist in input1"
        case g                    => s"Found duplicates - '${g}'"
      })

      persist(out.toTextFile(outputPath.path.path, overwrite = true))
      ()
    }
  }
}
