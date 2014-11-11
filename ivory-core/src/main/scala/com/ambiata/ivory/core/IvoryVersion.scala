package com.ambiata.ivory.core

import argonaut._, Argonaut._
import scalaz._

case class IvoryVersion(version: String)

object IvoryVersion {

  def get: IvoryVersion =
    IvoryVersion(BuildInfo.version)

  implicit def IvoryVersionEqual: Equal[IvoryVersion] =
    Equal.equalA[IvoryVersion]

  implicit def IvoryVersionCodecJson: CodecJson[IvoryVersion] = CodecJson.derived(
    EncodeJson(_.version.asJson),
    DecodeJson.optionDecoder(_.as[String].toOption.map(IvoryVersion.apply), "IvoryVersion")
  )
}
