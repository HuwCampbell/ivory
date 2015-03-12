package com.ambiata.ivory.operation.debug

import com.ambiata.ivory.core._

case class DumpFactsMapper(entities: Set[String], attributes: Set[String], source: String) {
  val delimiter = '|'
  val missing = "NA"

  def accept(fact: Fact): Boolean =
    (entities.contains(fact.entity) || entities.isEmpty) &&
      (attributes.contains(fact.featureId.toString) || attributes.isEmpty)

  def render(fact: Fact): String =
    renderWith(fact, new StringBuilder())

  def renderWith(fact: Fact, buffer: StringBuilder): String = {
    buffer.setLength(0)
    buffer.append(fact.entity)
    buffer.append(delimiter)
    buffer.append(fact.namespaceUnsafe.name)
    buffer.append(delimiter)
    buffer.append(fact.feature)
    buffer.append(delimiter)
    TextEscaping.escapeAppend(delimiter, Value.json.toStringWithStruct(fact.value, missing), buffer)
    buffer.append(delimiter)
    buffer.append(fact.datetime.localIso8601)
    buffer.append(delimiter)
    buffer.append(source)
    buffer.toString
  }
}
