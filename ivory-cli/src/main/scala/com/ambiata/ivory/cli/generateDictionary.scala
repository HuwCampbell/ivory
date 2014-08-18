package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._

import com.ambiata.ivory.generate._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.poacher.hdfs._

object generateDictionary extends IvoryApp {

  case class CliArguments(namespaces: Int, features: Int, output: String)

  val parser = new scopt.OptionParser[CliArguments]("generate-dictionary"){
    head("""
|Random Dictionary Generator.
|
|This app generates a random dictionary and feature flag files used to generate random features
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[Int]('n', "namespaces") action { (x, c) => c.copy(namespaces = x) } required() text s"Number of namespaces to generate."
    opt[Int]('f', "features")   action { (x, c) => c.copy(features = x) }   required() text s"Number of features to generate."
    opt[String]('o', "output")  action { (x, c) => c.copy(output = x) }     required() text s"Hdfs path to write dictionary to."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments(0, 0, ""), IvoryRunner { configuration => c =>
      generate(c.namespaces, c.features, new Path(c.output, "dictionary"), new Path(c.output, "flags")).run(configuration.configuration).map {
        case _ => List(s"Dictionary successfully written to ${c.output}.")
      }
    })

  def generate(namespaces: Int, features: Int, dictPath: Path, flagsPath: Path): Hdfs[Unit] = for {
    dict <- GenerateDictionary.onHdfs(namespaces, features)
    _    <- Hdfs.writeWith(dictPath, Streams.write(_, DictionaryTextStorage.delimitedString(dict)))
    _    <- GenerateFeatureFlags.onHdfs(dict, flagsPath)
  } yield ()
}
