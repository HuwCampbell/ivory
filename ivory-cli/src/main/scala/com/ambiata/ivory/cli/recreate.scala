package com.ambiata.ivory.cli

import com.ambiata.ivory.core.Repository
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import MemoryConversions._
import scalaz._, Scalaz._

object recreate extends IvoryApp {
  case class CliArguments(input: String, output: String, clean: Boolean, dry: Boolean, overwrite: Boolean, recreateData: List[RecreateData], maxNumber: Option[Int], reducerSize: Option[Long])

  val parser = new scopt.OptionParser[CliArguments]("recreate") {
    head("""Clone an ivory repository, recompressing each part file and storing in a the latest format.""")

    help("help") text "shows this usage text"

    opt[String]('i', "input")     action { (x, c) => c.copy(input = x) }     required() text "Input ivory repository."
    opt[String]('o', "output")    action { (x, c) => c.copy(output = x) }    required() text "Output ivory repository."
    opt[Unit]('d', "dry-run")     action { (_, c) => c.copy(dry = true) }    optional() text "Do a dry run only."
    opt[Unit]("no-clean")         action { (_, c) => c.copy(clean = false) } optional() text "Do not clean out empty factsets from feature stores."
    opt[String]('t', "type")      action { (x, c) => c.copy(recreateData = RecreateData.parse(x)) } optional() text "Type of data to recreate: dictionary, store, snapshot, factset, all (default)"
    opt[Int]('n', "number")       action { (x, c) => c.copy(maxNumber = Some(x)) } optional() text "Maximum number of elements to recreate."
    opt[Long]('s', "reducer-size") action { (x, c) => c.copy(reducerSize = Some(x)) } optional() text "Max size (in bytes) of a reducer used to copy Factsets"
    opt[Unit]('w', "overwrite")   action { (_, c) => c.copy(overwrite = true) } optional() text "Overwrite the destination repository."

  }


  val cmd = IvoryCmd[CliArguments](parser, CliArguments(input = "", output = "", clean = true, dry = false, overwrite = false, recreateData = RecreateData.ALL, maxNumber = None, reducerSize = None),
    IvoryRunner { configuration => c =>
      val rconf = RecreateConfig(from = Repository.fromHdfsPath(FilePath(c.input), configuration),
                                 to = Repository.fromHdfsPath(FilePath(c.output), configuration),
                                 sc = configuration.scoobiConfiguration,
                                 codec = configuration.codec,
                                 clean = c.clean,
                                 dry = c.dry,
                                 overwrite = c.overwrite,
                                 recreateData = c.recreateData,
                                 reducerSize = c.reducerSize.map(_.bytes).getOrElse(256.mb),
                                 maxNumber = c.maxNumber,
                                 logger = consoleLogging)

      Recreate.all.run(rconf).as(Nil)
    })

}
