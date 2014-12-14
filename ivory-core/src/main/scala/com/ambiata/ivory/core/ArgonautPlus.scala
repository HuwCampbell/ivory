package com.ambiata.ivory.core

import argonaut._, Argonaut._

object ArgonautPlus {
  def decodeEnum[A](label: String, cases: PartialFunction[String, A]): DecodeJson[A] =
    DecodeJson.optionDecoder(c => c.string.flatMap(cases.lift), label)

  def codecEnum[A](label: String, encode: A => String, cases: PartialFunction[String, A]): CodecJson[A] =
    CodecJson.derived(EncodeJson(encode andThen (_.asJson)), decodeEnum(label, cases))
}
