package com.ambiata.ivory.core

import java.nio.ByteBuffer

import com.ambiata.ivory.core.thrift._
import scodec.bits.ByteVector

/**
 * This class represents a parsing error, with the parsed line and the error message
 * An instance of this class can be serialised as a thrift record
 */
case class ParseError(message: String, data: ErrorData) {
  def toThrift = {
    new ThriftParseError(message, data match {
      case TextError(line)                          => ParseErrorData.text(new TextErrorData(line))
      // NOTE: Calling ByteVector.toByteBuffer makes it read-only, which means it can't be serialized later
      case ThriftError(ThriftErrorDataVersionV1, b) => ParseErrorData.thriftV1(new ThriftV1ErrorData(ByteBuffer.wrap(b.toArray)))
    })
  }
  def appendToMessage(msg: String) = copy(message = message + msg)
}

sealed trait ErrorData

case class TextError(line: String) extends ErrorData
case class ThriftError(version: ThriftErrorDataVersion, bytes: ByteVector) extends ErrorData

sealed trait ThriftErrorDataVersion
object ThriftErrorDataVersionV1 extends ThriftErrorDataVersion

object ParseError {
  def withLine(line: String) = (msg: String) => ParseError(msg, TextError(line))
  def fromThrift(t: ThriftParseError): ParseError =
    ParseError(t.message, t.data match {
      case x if x.isSetText     => TextError(x.getText.line)
      case x if x.isSetThriftV1 => ThriftError(ThriftErrorDataVersionV1, ByteVector(x.getThriftV1.getBytes))
    })
}
