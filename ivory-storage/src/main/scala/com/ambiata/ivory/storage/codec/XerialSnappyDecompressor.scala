package com.ambiata.ivory.storage.codec

import java.nio.ByteBuffer

import org.apache.hadoop.io.compress.Decompressor
import org.xerial.snappy.Snappy

/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class XerialSnappyDecompressor extends Decompressor {
  private var outputBuffer = ByteBuffer.allocateDirect(0)
  private var inputBuffer = ByteBuffer.allocateDirect(0)
  private var isFinished = false

  def setInput(buffer: Array[Byte], off: Int, len: Int): Unit = synchronized {
    if (inputBuffer.capacity() - inputBuffer.position() < len) {
      val newBuffer = ByteBuffer.allocateDirect(inputBuffer.position() + len)
      inputBuffer.rewind()
      newBuffer.put(inputBuffer)
      inputBuffer = newBuffer
    } else {
      inputBuffer.limit(inputBuffer.position() + len)
    }
    inputBuffer.put(buffer, off, len)
  }

  def needsDictionary(): Boolean = false

  def setDictionary(b: Array[Byte], off: Int, len: Int): Unit = {}

  def decompress(buffer: Array[Byte], off: Int, len: Int): Int = synchronized {
    if (!outputBuffer.hasRemaining) {
      inputBuffer.rewind()
      // There is compressed input, decompress it now.
      val decompressedSize = Snappy.uncompressedLength(inputBuffer)
      if (decompressedSize > outputBuffer.capacity()) {
        outputBuffer = ByteBuffer.allocateDirect(decompressedSize)
      }

      // Reset the previous outputBuffer (i.e. set position to 0)
      outputBuffer.clear()
      val size = Snappy.uncompress(inputBuffer, outputBuffer)
      outputBuffer.limit(size)
      // We've decompressed the entire input, reset the input now
      inputBuffer.clear()
      inputBuffer.limit(0)
      isFinished = true
    }

    // Return compressed output up to 'len'
    val numBytes = Math.min(len, outputBuffer.remaining())
    outputBuffer.get(buffer, off, numBytes)
    return numBytes
  }

  def getRemaining: Int = 0

  def finished(): Boolean = synchronized(isFinished && !outputBuffer.hasRemaining)

  def needsInput(): Boolean = synchronized(!inputBuffer.hasRemaining && !outputBuffer.hasRemaining)

  def reset(): Unit = synchronized {
    isFinished = false
    inputBuffer.rewind()
    outputBuffer.rewind()
    inputBuffer.limit(0)
    outputBuffer.limit(0)
  }

  def end(): Unit = {}
}
