package com.ambiata.ivory.core

/**
 * Container for expressions and an optional filter which is likely to evolve fairly drastically as more combinations
 * are supported.
 */
case class Query(expression: Expression, filter: Option[Filter])
