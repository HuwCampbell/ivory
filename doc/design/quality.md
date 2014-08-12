
Process
-------

 * Try to to avoid changes larger than 2 days without grabbing a couple
   of people to try and break it down into smaller chunks and avoid
   too much churn.

 * Try to get two sets of eyes on most PRs. For now this is @markhibberd
   for correctness/data integrity/performance/compatibility plus one
   other person for general quality. Ideally this should become any two
   people once everyone is better equipped (and we have more robust
   testing) for dealing with some of the downstream issues and concerns.

Design
------

 * Avoid internal type aliases, if there is a concept pull it out and
   share.

 * Remove duplication, in particular there is too much conceptual duplication
   in storage and similar areas (i.e. different instantiations per platform,
   per version when there could be just one thing properly abstracted).

 * Remove "hole" in the middle anti-pattern, split and compose.

 * Configuration goes in as arguments. Remove mix of "configuration" styles
   with implicits and readers. Any reader should be carefully considered and
   should have non-trivial gains across the entire code base, not just the
   code you are writing.

 * Consistent effect handling, unsafePerformIO's go at the top. Avoid
   implementation specific effects crossing logical boundaries (see
   more on this in clarity.md).


Syntax and Scala Things
-----------------------

 * No procedure syntax.

 * Explicit return types on anything that can be referenced outside
   of the class/trait/object.

 * Avoid overloading. Any overloaded method must be justified and
   provide some benefit over just having a different symbol/name.

 * No default params. No exceptions.

 * Avoid Any/Nothing quantification. Just use a type param.
 
 * Tend toward having the same file name as the main Scala class in it. 


Testing
-------

 * When refering to test methods from the s2 string, use short, some
   what representitive, names. Don't need to be totally descriptive
   as we have the test description, but should be an easier hint to
   refer back to then the e{1,2,3} style.

 * Better test descriptions. As a guide, should include the input
   scenario and the expected outcome (not just the input).

 * Properties should be small - aim for a single invariant per
   property.

 * Arbitraries should be "sharable" concepts. Named after the data
   pattern they generate, not the types they include.


Documentation
-------------

 * Any non-trivial algorithm or core concept should have a block comment
   explaining, what it is, and what it isn't, and any invariants that should
   hold.
