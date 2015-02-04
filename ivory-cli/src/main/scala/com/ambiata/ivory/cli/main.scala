package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import scalaz._, Scalaz._

object main {

  val commands: List[IvoryApp] = List(
    admin.renameFacts,
    catDictionary,
    catErrors,
    chord,
    config,
    convertDictionary,
    countFacts,
    createRepository,
    debug.catThrift,
    debug.dumpFacts,
    debug.dumpReduction,
    health.recreate,
    importDictionary,
    ingest,
    snapshot,
    statsFactset,
    update
  )

  def main(args: Array[String]): Unit = {
    handleVersionAndExit(args)
    val program: Option[RIO[Option[Unit]]] = for {
      (progName, argsRest) <- args.headOption.map(_ -> args.tail)
      command <- commands.find(_.cmd.parser.programName == progName)
    } yield command.cmd.run(argsRest)
    // End of the universe
    program.sequence.flatMap(o => o.flatten.fold(usage())(_ => RIO.unit)).unsafePerformIO match {
      case Ok(_) =>
        ()
      case Error(e) =>
        sys.error(Result.asString(e))
    }
  }

  def usage(): RIO[Unit] = RIO.safe {
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
