package com.ambiata.ivory.cli

import com.ambiata.ivory.operation.update._
import com.ambiata.ivory.storage.control.IvoryT

import pirate._

import scalaz._, Scalaz._

object update extends IvoryApp {

  val cmd = Command(
    "update"
  , Some("""
    |Update to the latest ivory metadata version.
    |""".stripMargin)
  , IvoryCmd.repositoryBypassVersionCheck.map(r => IvoryRunner(configuration =>
      IvoryT.fromRIO(r(configuration)).flatMap(repo => Update.update.toIvoryT(repo).as(Nil))
  )))
}
