package com.ambiata.ivory.core

import scalaz._, Scalaz._

/** The string representation of the filter until such time that we can parse the Dictionary in two passes. */
case class Filter(render: String)

/** The _real_ filter, which can _only_ be safely constructed with knowledge of the concrete encoding. */
sealed trait FilterEncoded
case class FilterValues(op: FilterOp, fields: List[FilterExpression]) extends FilterEncoded
case class FilterStruct(op: FilterOp, fields: List[(String, FilterExpression)]) extends FilterEncoded

sealed trait FilterOp
case object FilterOpAnd extends FilterOp
case object FilterOpOr extends FilterOp

sealed trait FilterExpression
case class FilterEquals(value: PrimitiveValue) extends FilterExpression

/**
 * Current format is _intentionally_ crippled to set expectations that it will change _very_ shortly.
 * This isn't even V1 of the text format; yes it's _that_ bad.
 *
 * Currently it only supports a single operation `and` or `or`, and no nesting.
 *
 * {{{
 *  ns1:foo|encoding=int
 *  ns1:bar|source=ns1:foo|expression=count|window=2 months|filter=or,1,2,3
 *  ns2:foo|encoding=(a: string, b: int)
 *  ns2:bar|source=ns2:foo|expression=count|window=2 months|filter=and,a,hello,a,goodbye,b,10
 * }}}
 */
object FilterTextV0 {

  object FilterOpTextV0 {

    def parse(op: String): String \/ FilterOp = op match {
      case "and" => FilterOpAnd.right
      case "or"  => FilterOpOr.right
      case _     => s"Invalid filter operation '$op'".left
    }

    def asString(op: FilterOp): String = op match {
      case FilterOpAnd => "and"
      case FilterOpOr  => "or"
    }
  }

  object FilterExpressionTextV0 {

    // Among many things, we're assuming that values to compare can never equal a tombstone
    def parse(encoding: PrimitiveEncoding, value: String): String \/ FilterExpression =
       Value.parsePrimitive(encoding, value).disjunction.map(FilterEquals.apply)
 
    /** This won't make any sense until we add more expression types */
    def asString(exp: FilterExpression): String = exp match {
      case FilterEquals(value) => Value.toStringPrimitive(value)
    }
  }

  def encode(filter: Filter, encoding: Encoding): String \/ FilterEncoded = {
    filter.render.split(",", -1).toList match {
      case Nil              => "Invalid filter: must not be blank".left
      case opString :: tail =>
        encoding match {
          case StructEncoding(values) => for {
            op <- FilterOpTextV0.parse(opString)
            fields <- tail.grouped(2).toList.traverseU {
              case List(field, value) =>
                values.get(field)
                  .toRightDisjunction(s"Invalid filter: struct field $field not found")
                  .flatMap(se => FilterExpressionTextV0.parse(se.encoding, value).map(field ->))
              case _ => "Invalid filter: struct filters require name/value pairs".left
            }
          } yield FilterStruct(op, fields)
          case pe: PrimitiveEncoding => for {
            op <- FilterOpTextV0.parse(opString)
            fields <- tail.traverseU(FilterExpressionTextV0.parse(pe, _))
          } yield FilterValues(op, fields)
          case _: ListEncoding => "Filtering lists is not yet supported".left
        }
    }
  }

  def asString(filter: FilterEncoded): Filter = Filter(
    (filter match {
      case FilterValues(op, fields) =>
        FilterOpTextV0.asString(op) :: fields.map(FilterExpressionTextV0.asString)
      case FilterStruct(op, fields) =>
        FilterOpTextV0.asString(op) :: fields.flatMap(f => List(f._1, FilterExpressionTextV0.asString(f._2)))
    }).mkString(",")
  )
}
