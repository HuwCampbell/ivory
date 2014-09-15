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
    convertDictionary,
    countFacts,
    createRepository,
    factDiff,
    importDictionary,
    ingest,
    pivotSnapshot,
    recompress,
    recreate,
    snapshot
  )

  def main(args: Array[String]): Unit = {
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
}
