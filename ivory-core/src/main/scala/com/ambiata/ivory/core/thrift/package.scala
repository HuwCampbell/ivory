package com.ambiata.ivory.core

package object thrift {
  type ThriftLike = org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_, _], _ <: org.apache.thrift.TFieldIdEnum]
}
