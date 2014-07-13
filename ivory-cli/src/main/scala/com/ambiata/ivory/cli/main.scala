package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._, effect._

object main {

  val commands: List[IvoryApp] = List(
    catDictionary,
    catErrors,
    catFacts,
    catRepositoryFacts,
    chord,
    countFacts,
    createFeatureStore,
    createRepository,
    factDiff,
    generateDictionary,
    generateFacts,
    importDictionary,
    importFacts,
    importFeatureStore,
    ingest,
    ingestBulk,
    pivot,
    pivotSnapshot,
    recompress,
    snapshot,
    validateFactSet,
    validateStore
  )

  def main(args: Array[String]): Unit = {
    val program = for {
      (progName, argsRest) <- args.headOption.map(_ -> args.tail)
      command <- commands.find(_.cmd.name == progName)
    } yield run(command.cmd)(argsRest)
    // End of the universe
    program.getOrElse(usage()).flatMapError { e =>
      ResultT.safe {
        println(s"Failed! - ${Result.asString(e)}")
        sys.exit(1)
      }}.run.unsafePerformIO()
  }

  def usage(): ResultTIO[Unit] = ResultT.safe {
    val cmdNames = commands.map(_.cmd.name).mkString("|")
    println(s"Usage: {$cmdNames}")
  }

  def run[A](app: IvoryCmdParser): Array[String] => ResultTIO[Unit] = { args =>
    def printLines(result: ResultTIO[List[String]]): ResultTIO[Unit] =
      result.flatMap(lines => ResultT.safe(println(lines)))
    // TODO This won't work for ScoobiCmd with non-declared params
    app.parse(args).flatMap {
      case ActionCmd(f) => printLines(f.executeT(consoleLogging).map(_ => Nil))
      case HadoopCmd(f) => printLines(f(new Configuration()))
      // TODO Simply usage of ScoobiApp where possible
      // https://github.com/ambiata/ivory/issues/27
      case ScoobiCmd(f) => ResultT.safe {
        // Hack to avoid having to copy logic from ScoobiApp
        // To make this harder we need ScoobiApp to remove its own args before we begin
        // May Tony have mercy on my soul
        new ScoobiApp { self =>
          // Need to evaluate immediately to avoid the main() method of ScoobiApp cleaning up too soon
          def run() = printLines(f(configuration)).run.unsafePerformIO()
        }.main(args)
      }
    }
  }
}

case class IvoryCmd[A](parser: scopt.OptionParser[A], initial: A, runner: A => IvoryRunner) extends IvoryCmdParser {

  def name = parser.programName

  // TODO Process Option correctly!
  def parse(args: Seq[String]) =
    ResultT.safe[IO, A](parser.parse(args, initial).get).map(runner)
}

/**
 * Represents the different types of runners in an Ivory program,
 * so that any required setup can be handled in a single place
 */
sealed trait IvoryRunner
case class ActionCmd(f: IOAction[Unit]) extends IvoryRunner
case class HadoopCmd(f: Configuration => ResultTIO[List[String]]) extends IvoryRunner
case class ScoobiCmd(f: ScoobiConfiguration => ResultTIO[List[String]]) extends IvoryRunner

trait IvoryApp {
  def cmd: IvoryCmdParser
}

trait IvoryCmdParser {
  def name: String
  def parse(args: Seq[String]): ResultTIO[IvoryRunner]
}

import io.mth.pirate._, Pirate._

case class IvoryPirateCmd(cmd: Command[IvoryRunner]) extends IvoryCmdParser {

  def name = cmd.name

  def parse(args: Seq[String]) =
    ResultT.fromDisjunction[IO, IvoryRunner](Interpretter.run(cmd.parse, args.toList).leftMap {
      case PErrorNoMessage => \&/.This("")
      case PErrorMessage(m) => \&/.This(m)
    }).flatMapError {
      error =>
        // TODO Remove the IO completely once we can get rid of stupid scopt
        ResultT.safe[IO, Unit](println(Usage.print(cmd))).flatMap(_ => ResultT.fromDisjunction(error.left))
    }
}
