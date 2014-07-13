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

  def run[A](app: IvoryCmdParser[A]): Array[String] => ResultTIO[Unit] = { args =>
    def parseAndRun(args: Seq[String], result: A => ResultTIO[List[String]]): ResultTIO[Unit] =
      (app.parse(args) >>= result).flatMap(lines => ResultT.safe(println(lines)))
    app.runner match {
      case ActionCmd(f) => parseAndRun(args, f andThen (_.executeT(consoleLogging).map(_ => Nil)))
      case HadoopCmd(f) => parseAndRun(args, f(new Configuration()))
      // TODO Simply usage of ScoobiApp where possible
      // https://github.com/ambiata/ivory/issues/27
      case ScoobiCmd(f) => ResultT.safe {
        // Hack to avoid having to copy logic from ScoobiApp
        // To make this harder we need ScoobiApp to remove its own args before we begin
        // May Tony have mercy on my soul
        new ScoobiApp {
          // Need to evaluate immediately to avoid the main() method of ScoobiApp cleaning up too soon
          def run() = parseAndRun(args, f(configuration)).run.unsafePerformIO()
        }.main(args)
      }
    }
  }
}

case class IvoryCmd[A](parser: scopt.OptionParser[A], initial: A, runner: IvoryRunner[A]) extends IvoryCmdParser[A] {

  def name = parser.programName

  // TODO Process Option correctly!
  def parse(args: Seq[String]) =
    ResultT.safe[IO, A](parser.parse(args, initial).get)
}

/**
 * Represents the different types of runners in an Ivory program,
 * so that any required setup can be handled in a single place
 */
sealed trait IvoryRunner[A]
case class ActionCmd[A](f: A => IOAction[Unit]) extends IvoryRunner[A]
case class HadoopCmd[A](f: Configuration => A => ResultTIO[List[String]]) extends IvoryRunner[A]
case class ScoobiCmd[A](f: ScoobiConfiguration => A => ResultTIO[List[String]]) extends IvoryRunner[A]

trait IvoryApp {
  def cmd: IvoryCmdParser[_]
}

trait IvoryCmdParser[A] {
  def name: String
  def runner: IvoryRunner[A]
  def parse(args: Seq[String]): ResultTIO[A]
}

import io.mth.pirate._, Pirate._

case class IvoryPirateCmd[A](parser: Command[A], runner: IvoryRunner[A]) extends IvoryCmdParser[A] {

  def name = parser.name

  def parse(args: Seq[String]) =
    ResultT.fromDisjunction[IO, A](Interpretter.run(parser, args.toList).leftMap {
      case PErrorNoMessage => \&/.This("")
      case PErrorMessage(m) => \&/.This(m)
    }).flatMapError {
      error =>
        // TODO Remove the IO completely once we can get rid of stupid scopt
        ResultT.safe[IO, Unit](println(Usage.print(Command("ivory", None, parser)))).flatMap(_ => ResultT.fromDisjunction(error.left))
    }
}
