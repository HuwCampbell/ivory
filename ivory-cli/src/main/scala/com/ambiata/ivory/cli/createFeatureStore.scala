package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._
import org.apache.hadoop.fs.Path

object createFeatureStore extends IvoryApp {

  case class CliArguments(repo: String, name: String, sets: String, existing: Option[String])

  val parser = new scopt.OptionParser[CliArguments]("create-feature-store"){
    head("""
|Create a new feature store in an ivory repository.
|
|This app will create a new feature store, optionally appending an existing one to the end.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) } required() text
      s"Ivory repository to create."

    opt[String]('n', "name")            action { (x, c) => c.copy(name = x) } required() text s"Name of the feature store in the repository."
    opt[String]('s', "sets")            action { (x, c) => c.copy(sets = x) } required() text s"Comma separated list of fact sets to use in this feature store."
    opt[String]('e', "append-existing") action { (x, c) => c.copy(existing = Some(x)) }  text s"Name of an existing feature store to append to the end of this one."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", "", None), HadoopCmd { configuration => c =>
    val sets = c.sets.split(",").toList.map(_.trim).map(Factset)

    val actions =
      CreateFeatureStore.onHdfs(new Path(c.repo), c.name, sets, c.existing).run(configuration)

    actions.map {
      case _ => List(s"Successfully created feature store in ${c.repo} under the name ${c.name}.")
    }
  })
}
