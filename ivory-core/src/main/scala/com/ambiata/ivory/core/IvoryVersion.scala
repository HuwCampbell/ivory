package com.ambiata.ivory.core

import argonaut._, Argonaut._

case class IvoryVersion(version: String)

object IvoryVersion {

  def get: IvoryVersion =
    IvoryVersion(BuildInfo.version)

  implicit def IvoryVersionCodecJson: CodecJson[IvoryVersion] = CodecJson.derived(
    EncodeJson(_.version.asJson),
    DecodeJson.optionDecoder(_.as[String].toOption.map(IvoryVersion.apply), "IvoryVersion")
  )
}
