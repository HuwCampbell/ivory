package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire._
import scalaz._,Scalaz._

object compareSnapshots extends IvoryApp {

  case class CliArguments(snap1: String, snap2: String, output: String)

  val parser = new scopt.OptionParser[CliArguments]("compare-snapshots") {
    head("""
         |App to compare two snapshots
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]("snap1")  action { (x, c) => c.copy(snap1 = x) }  required() text s"Hdfs path to the first snapshot."
    opt[String]("snap2")  action { (x, c) => c.copy(snap2 = x) }  required() text s"Hdfs path to the second snapshot."
    opt[String]("output") action { (x, c) => c.copy(output = x) } required() text s"Hdfs path to store results."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", ""), IvoryRunner {
    configuration => c =>
      val banner = s"""======================= CompareSnapshot =======================
                      |
                      |Arguments --
                      |
                      |  Snap1                    : ${c.snap1}
                      |  Snap2                    : ${c.snap2}
                      |  Output                   : ${c.output}
                      |
                      |""".stripMargin
      println(banner)

    compareHdfsSnapshots(c.snap1, c.snap2, c.output, configuration).as(List(banner, s"Output path: $c.output", "Status -- SUCCESS"))
  })
}
