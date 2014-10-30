package com.ambiata.ivory.mr

import org.specs2._

class CounterSpec extends Specification with ScalaCheck { def is = s2"""

Counter Smoke Tests
-------------------

These are not incredibly useful, but better than nothing...

  Counter can count                              $count
  LabelledCounter can count                      $labelled
  Memory matches from function counter           $memory
  Labelled memory matches from function counter  $memorylabelled

"""

  def count = prop((n: List[Int]) => {
    var x = 0
    val counter = Counter(x += _)
    n.foreach(counter.count)
    x must_== n.sum
  })

  def labelled = prop((s: String, n: Int, ns: List[Int], t: String, m: Int, ms: List[Int]) => (s != t) ==> {
    var x = Map.empty[String, Int]
    val counter = LabelledCounter((l, c) => x = x + (l -> (x.getOrElse(l, 0) + c)))
    (n :: ns).foreach(counter.count(s, _))
    (m :: ms).foreach(counter.count(t, _))
    x must_== Map(s -> (n + ns.sum), t -> (m + ms.sum))
  })

  def memory = prop((n: List[Int]) => {
    var x = 0
    val counter = Counter(x += _)
    val memory = MemoryCounter()
    n.foreach(z => { counter.count(z); memory.count(z) })
    memory.counter must_== x
  })

  def memorylabelled = prop((s: String, n: List[Int], t: String, m: List[Int]) => {
    var x = Map.empty[String, Int]
    val counter = LabelledCounter((l, c) => x = x + (l -> (x.getOrElse(l, 0) + c)))
    val memory = MemoryLabelledCounter()
    n.foreach(z => { counter.count(s, z); memory.count(s, z) })
    m.foreach(z => { counter.count(t, z); memory.count(t, z) })
    memory.counters must_== x
  })
}
