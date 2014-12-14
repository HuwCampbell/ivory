package com.ambiata.ivory.core

object Crash {
  sealed trait Scope
  case object DataIntegrity       extends Scope
  case object Serialization       extends Scope
  case object CodeGeneration      extends Scope
  case object Invariant           extends Scope
  case object RIO                 extends Scope // These should be fixed to actually be RIO errors

  def error(scope: Scope, message: String): Nothing =
    sys.error(s"""################# Critical Ivory Error #################
                 |
                 |${reason(scope)}
                 |
                 |${message}
                 |""".stripMargin)

  val genericMessage: String =
     """This is most likely an ivory code issue, raise an issue at
        |https://github.com/ambiata/ivory/issues including the complete stack trace
        |for this error.
        """.stripMargin

  def reason(scope: Scope): String =
    scope match {
      case DataIntegrity =>
        """A situation has occurred where a potential data integrity issue has been found
          |and processing has been terminated. This can happen if an incompatible dictionary
          |change has been 'forced'.
          |""".stripMargin
      case Serialization =>
        """An internal serialization issue has occurred. This can happen if temporary
          |data has been cleared whilst a job is running. Re-try the job, and if the
          |issue continues raise an issue at https://github.com/ambiata/ivory/issues
          |including the complete stack trace for this error.
          |""".stripMargin
      case CodeGeneration =>
        s"""An internal invariant has been violated due to incorrect or out of date
           |handling of generated code.
           |${genericMessage}
           |""".stripMargin
      case Invariant =>
        s"""An internal invariant has been violated.
           |${genericMessage}
           |""".stripMargin
      case RIO =>
        """Unhandled ivory error. More detail included below.
          |""".stripMargin
    }

  def raiseIssue: String =
    """Please raise an issue at the following location: https://github.com/ambiata/ivory/issues"""
}
