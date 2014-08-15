package com.ambiata.ivory.operation.extraction

import java.util.{Iterator => JIterator}

import com.ambiata.ivory.core.thrift._
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.thrift.TDeserializer
import org.apache.thrift.protocol.TCompactProtocol

/** Encapsulate the dirty mutation of a priority tag, USE WITH CARE!!! */
class PriorityTagDeserializer[A](fact: A)(implicit ev: A <:< ThriftLike) {

  /** Empty PriorityTag, created once per reducer and mutated per record */
  val priorityTag = new PriorityTag

  /** Thrift deserializer */
  val serializer = ThriftSerialiser()

  /** Returns the highest priority tag, and will only return null if iter is empty  */
  def findHighestPriority(state: PriorityTagDeserializer.State[A], kout: NullWritable,
                          iter: JIterator[BytesWritable]): PriorityTag = {
    var latestContainer: PriorityTag = null
    while(iter.hasNext) {
      val next = iter.next
      serializer.fromBytesUnsafe(priorityTag, next.getBytes) // populate PriorityTag which holds priority and serialized fact
      serializer.fromBytesUnsafe(fact, priorityTag.getBytes) // populate fact
      if (latestContainer == null || state.accept(fact, priorityTag.getPriority < latestContainer.getPriority)) {
        latestContainer = priorityTag.deepCopy
        state.save(fact)
      }
    }
    latestContainer
  }
}

object PriorityTagDeserializer {

  trait State[A] {
    def accept(fact: A, higherPriority: Boolean): Boolean
    def save(fact: A): Unit
  }
}
