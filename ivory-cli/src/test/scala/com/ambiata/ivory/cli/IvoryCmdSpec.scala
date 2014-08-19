package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.repository.RepositoryConfiguration
import com.ambiata.mundane.control.ResultT
import org.specs2.Specification
import org.specs2.execute.AsResult
import org.specs2.matcher.ThrownExpectations

import scalaz.effect.IO

class IvoryCmdSpec extends Specification with ThrownExpectations { def is = sequential ^ s2"""
  The IvoryCmd creates a RepositoryConfiguration instance with
    the user arguments                                          $userArguments
    a Configuration set-up with Hadoop arguments                $hadoopArguments
    a ScoobiConfiguration set-up with Scoobi arguments          $scoobiArguments
"""

  val arguments = "-Dmapreduce.map.memory.mb=1546 --number 1 scoobi local.verbose.all"

  def userArguments =
    run(arguments) { (rc: RepositoryConfiguration, number: Int) =>
      "the user arguments are correctly parsed" ==> {
        number must_== 1
      }
    }


  def hadoopArguments =
    run(arguments) { (rc: RepositoryConfiguration, number: Int) =>
      "the hadoop arguments are correctly parsed" ==> {
        rc.configuration.getInt("mapreduce.map.memory.mb", 0) must_== 1546
      }
    }

  def scoobiArguments =
    run(arguments) { (rc: RepositoryConfiguration, number: Int) =>
      "the scoobi arguments are correctly parsed" ==> {
        rc.scoobiConfiguration.isLocal
      }
    }


  def run[R : AsResult](arguments: String)(f: (RepositoryConfiguration, Int) => R) = {
    val args = arguments.split(" ")
    val optionsParser = new scopt.OptionParser[Int]("parser") {
      opt[Int]("number").action((i, n) => i)
    }
    val command = new IvoryCmd[Int](optionsParser, 0, IvoryRunner[Int](c => i => { f(c, i); ResultT.ok[IO, List[String]](Nil) }))
    command.run(args).unsafePerformIO
    ok
  }

}
