package com.ambiata.ivory.storage.codec

import java.io.{InputStream, OutputStream}

import org.apache.hadoop.conf.{Configuration, Configurable}
import org.apache.hadoop.fs.CommonConfigurationKeys
import org.apache.hadoop.io.compress._

class XerialSnappyCodec extends CompressionCodec with Configurable {
  private var configuration = new Configuration
  def setConf(c: Configuration) = { configuration = c }
  def getConf = configuration


  def createOutputStream(out: OutputStream): CompressionOutputStream =
    createOutputStream(out, createCompressor)

  def getDefaultExtension: String = ".snappy"

  def getCompressorType: Class[_ <: Compressor] = classOf[XerialSnappyCompressor]

  def createInputStream(in: InputStream): CompressionInputStream =
    createInputStream(in, createDecompressor)

  def createInputStream(in: InputStream, decompressor: Decompressor): CompressionInputStream = {
    return new BlockDecompressorStream(in, decompressor, configuration.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY, CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT))
  }

  def createCompressor(): Compressor = new XerialSnappyCompressor

  def createOutputStream(out: OutputStream, compressor: Compressor): CompressionOutputStream = {
    val bufferSize: Int = configuration.getInt(CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY, CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT)
    val compressionOverhead: Int = (bufferSize / 6) + 32
    new BlockCompressorStream(out, compressor, bufferSize, compressionOverhead)
  }

  def getDecompressorType: Class[_ <: Decompressor] = classOf[XerialSnappyDecompressor]
  def createDecompressor(): Decompressor = new XerialSnappyDecompressor
}

