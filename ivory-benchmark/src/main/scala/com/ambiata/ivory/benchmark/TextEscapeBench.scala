package com.ambiata.ivory.benchmark

import com.ambiata.ivory.core._
import com.google.caliper._

object TextEscapeBenchApp extends App {
  Runner.main(classOf[TextEscapeBench], args)
}

class TextEscapeBench extends SimpleScalaBenchmark {

  val string = (("a" * 10) + "|") * 10

  def time_append(n: Int) =
    repeat(n) {
      val s = new StringBuilder
      s.append(string)
      s.length
    }

  def time_append_escape(n: Int) =
    repeat(n) {
      val s = new StringBuilder
      TextEscaping.escapeAppend('|', string, s)
      s.length
    }

  def time_split(n: Int) =
    repeat(n) {
      string.split('|').length
    }

  def time_split_escape(n: Int) =
    repeat(n) {
      TextEscaping.split('|', string).length
    }
}
