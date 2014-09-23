package com.ambiata.ivory.operation.diff

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy.SnapshotStorageV1._
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.poacher.scoobi.ScoobiAction
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.lib.Relational
import org.apache.hadoop.fs.Path

import scalaz.{\/-, -\/, \/}

/**
 * Compare 2 snapshots using Scoobi
 */
object CompareSnapshots {

  def compareHdfsSnapshots(snapshotPath1: String, snapshotPath2: String, outputPath: String, configuration: IvoryConfiguration): ResultTIO[Unit] = {
    val action: ScoobiAction[Unit] = for {
      snap1 <- snapshotFromHdfs(new Path(snapshotPath1))
      snap2 <- snapshotFromHdfs(new Path(snapshotPath2))
      _     <- ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>
        val dlist1: DList[(String, String)] = snap1.map(byKey("snap1"))
        val dlist2: DList[(String, String)] = snap2.map(byKey("snap2"))

        val diff = Relational(dlist1).joinFullOuter(dlist2, missing("snap2"), missing("snap1"), compare).mapFlatten {
          case (_, err) => err
        }

        diff.toTextFile(outputPath).persist
      }
    } yield ()
    action.run(configuration.scoobiConfiguration)
  }

  def compare(eat: String, v1: String, v2: String): Option[String] =
    if(v1 != v2) Some(s"'${eat}' has value '${v1}' in snapshot1, but '${v2}' in snapshot2") else None

  def missing(name: String)(eavt: String, v: String): Option[String] =
    Some(eavt + " does not exist in " + name)

  def byKey(name: String)(f: ParseError \/ Fact): (String, String) = f match {
    case -\/(e) => Crash.error(Crash.DataIntegrity, s"Can not parse one of that facts in ${name}")
    case \/-(f) => (s"${f.entity}|${f.namespace}|${f.feature}|${f.datetime.localIso8601}", f.value.toString)
  }
}
