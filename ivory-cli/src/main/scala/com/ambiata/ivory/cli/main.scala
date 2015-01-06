package com.ambiata.ivory.cli

import scalaz._, Scalaz._
import scalaz.effect._

object main {

  val commands: List[IvoryApp] = List(
    admin.renameFacts,
    catDictionary,
    catErrors,
    catFacts,
    chord,
    config,
    convertDictionary,
    countFacts,
    createRepository,
    debug.catThrift,
    debug.dumpFacts,
    debug.dumpReduction,
    importDictionary,
    ingest,
    recompress,
    recreate,
    snapshot,
    statsFactset,
    update
  )

  def main(args: Array[String]): Unit = {
    handleVersionAndExit(args)
    val program = for {
      (progName, argsRest) <- args.headOption.map(_ -> args.tail)
      command <- commands.find(_.cmd.parser.programName == progName)
    } yield command.cmd.run(argsRest)
    // End of the universe
    program.sequence.flatMap(_.flatten.fold(usage())(_ => IO.ioUnit)).unsafePerformIO
  }

  def usage(): IO[Unit] = IO {
    val cmdNames = commands.map(_.cmd.parser.programName).mkString("|")
    println(s"Ivory ${BuildInfo.version}")
    println(s"Usage: {$cmdNames}")
    sys.exit(1)
  }

  // We could also mutate the scopt OptionParser, but this is a little more obvious
  def handleVersionAndExit(args: Array[String]): Unit =
    args match {
      case Array("--version") =>
        println(s"Ivory ${BuildInfo.version}")
        sys.exit(0)
      case _ =>
    }
}
