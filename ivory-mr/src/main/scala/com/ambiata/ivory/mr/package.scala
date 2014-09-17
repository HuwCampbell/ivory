package com.ambiata.ivory

import com.ambiata.ivory.core._

package object mr {

  type FactMutator[I, O] = MutableStream[MutableFact, I] with Mutator[MutableFact, O]
  type PipeFactMutator[I, O] = MutableStream[MutableFact, I] with PipeMutator[I, O]
}
