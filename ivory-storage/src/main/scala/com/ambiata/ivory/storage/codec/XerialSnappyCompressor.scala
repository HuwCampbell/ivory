package com.ambiata.ivory.storage.codec

import java.nio.ByteBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.Compressor
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
class XerialSnappyCompressor extends Compressor {

  private var outputBuffer = ByteBuffer.allocateDirect(0)
  private var inputBuffer = ByteBuffer.allocateDirect(0)

  private var bytesRead = 0L
  private var bytesWritten = 0L
  private var finishCalled = false

  def compress(buffer: Array[Byte], off: Int, len: Int) = synchronized {
    if (needsInput()) 0
    else {
      if (!outputBuffer.hasRemaining) {
        // There is uncompressed input, compress it now
        val maxOutputSize = Snappy.maxCompressedLength(inputBuffer.position())
        if (maxOutputSize > outputBuffer.capacity()) {
          outputBuffer = ByteBuffer.allocateDirect(maxOutputSize)
        }
        outputBuffer.clear()
        inputBuffer.limit(inputBuffer.position())
        inputBuffer.position(0)

        val size = Snappy.compress(inputBuffer, outputBuffer)
        outputBuffer.limit(size)
        inputBuffer.limit(0)
        inputBuffer.rewind()
      }

      // Return compressed output up to 'len'
      val numBytes = Math.min(len, outputBuffer.remaining())
      outputBuffer.get(buffer, off, numBytes)
      bytesWritten += numBytes
      numBytes
    }
  }

  def setInput(buffer: Array[Byte], off: Int, len: Int) =  synchronized {
    if (inputBuffer.capacity() - inputBuffer.position() < len) {
      val tmp = ByteBuffer.allocateDirect(inputBuffer.position() + len)
      inputBuffer.rewind()
      tmp.put(inputBuffer)
      inputBuffer = tmp
    } else {
      inputBuffer.limit(inputBuffer.position() + len)
    }

    inputBuffer.put(buffer, off, len)
    bytesRead += len
  }

  def end() {}

  def finish() { finishCalled = true }

  def finished() = synchronized {
    finishCalled && inputBuffer.position() == 0 && !outputBuffer.hasRemaining()
  }

  def getBytesRead = bytesRead
  def getBytesWritten = bytesWritten

  def needsInput = synchronized(!finishCalled)

  def reinit(c: Configuration ) {
    reset()
  }

  def reset() = synchronized {
    finishCalled = false
    bytesRead = 0L
    bytesWritten = 0
    inputBuffer.rewind()
    outputBuffer.rewind()
    inputBuffer.limit(0)
    outputBuffer.limit(0)
  }

  def setDictionary(dictionary: Array[Byte], off: Int, len: Int) { }
}
