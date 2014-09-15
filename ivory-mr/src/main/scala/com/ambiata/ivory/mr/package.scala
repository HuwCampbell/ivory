package com.ambiata.ivory

import com.ambiata.ivory.core._

package object mr {

  type PipeFactMutator[I, O] = MutableStream[MutableFact, I] with PipeMutator[I, O]
}
