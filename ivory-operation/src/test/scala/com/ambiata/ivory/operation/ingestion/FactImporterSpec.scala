package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._

class FactImporterSpec extends Specification with ScalaCheck { def is = s2"""

  Validate namespaces success                        $validateNamespacesSuccess
  Validate namespaces fail                           $validateNamespacesFail
"""

  def validateNamespacesSuccess = prop((dict: Dictionary) => {
    FactImporter.validateNamespaces(dict, dict.byFeatureId.keys.toList.map(_.namespace)).toEither must beRight
  })

  def validateNamespacesFail = prop((dict: Dictionary, names: List[Namespace]) => {
    // Lazy way of create at least one name that isn't in the dictionary
    val name = Namespace.unsafe(dict.definitions.map(_.featureId.namespace.name).mkString)
    val allNames = (name :: names).filter(dict.forNamespace(_).definitions.isEmpty)
    FactImporter.validateNamespaces(dict, allNames).toEither must beLeft
  })
}
