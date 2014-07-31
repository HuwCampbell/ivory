package com.ambiata.ivory.performance

import java.io._
import java.util.zip.GZIPInputStream
import scala.io.{Codec, Source}

object Gsod {

  def main(args: Array[String]): Unit = {
    val Array(dir, start, end, dest) = args
    val (s, e) = (start.toInt, end.toInt)
    val t2 = System.currentTimeMillis()
    val yearFolders = new File(dir).listFiles().filter(c => c.getName.toInt >= s && c.getName.toInt <= e)

    val destFile = new File(dest)
    destFile.mkdirs()
    yearFolders.toList.par.foreach { dir =>
      println(s"Processing ${dir.getName}")
      // I tried using scalaz-stream, but this turned out to be easier...
      // Output in a directory format ready for ivory ingest
      val f = new File(destFile, dir.getName + "/gsod/" + "input.psv")
      f.getParentFile.mkdirs()
      val fw = new FileWriter(f)
      try
        Option(dir.listFiles()).getOrElse(Array()).toList.filter(_.getName.endsWith("gz")).foreach { child =>
          Source.fromInputStream(new GZIPInputStream(new FileInputStream(child)))(Codec("UTF-8")).getLines().drop(1).foreach {
            lines(_).foreach { line => fw.append(line).append("\n")}
          }
        }
      finally fw.close()
    }
    println(s"Time taken: ${System.currentTimeMillis() - t2}ms")
  }

  def lines(line: String): List[String] = {
    // ftp://ftp.ncdc.noaa.gov/pub/data/gsod/GSOD_DESC.txt
    val stn = line.substring(0, 6)
    val wban = line.substring(7, 12)
    val ent = stn + '-' + wban
    val date = line.substring(14, 18) + "-" + line.substring(18, 20) + "-" + line.substring(20, 22)
    def add(f: String, v: String) = List(ent, f, v.trim, date)
    List(
      add("temp", line.substring(24, 30)),
      add("dewp", line.substring(35, 41)),
      add("slp", line.substring(46, 52)),
      add("stp", line.substring(57, 63)),
      add("visib", line.substring(68, 73)),
      add("wdsp", line.substring(78, 83)),
      add("mxspd", line.substring(88, 93)),
      add("gust", line.substring(95, 100)),
      add("max", line.substring(102, 108)),
      add("min", line.substring(110, 116)),
      add("prcp", line.substring(117, 123)),
      add("prcp_flag", line.substring(123, 124)),
      add("sndp", line.substring(125, 130)),
      add("fog", (line.charAt(132) == '1').toString),
      add("rain", (line.charAt(133) == '1').toString),
      add("snow", (line.charAt(134) == '1').toString),
      add("hail", (line.charAt(135) == '1').toString),
      add("thunder", (line.charAt(136) == '1').toString),
      add("tornado", (line.charAt(137) == '1').toString)
    ).map(_.mkString("|"))
  }
}