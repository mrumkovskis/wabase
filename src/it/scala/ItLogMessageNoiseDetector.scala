package org.wabase

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.boolex.EventEvaluatorBase

/** Detects expected error messages to hide when running integration tests */
class ItLogMessageNoiseDetector extends EventEvaluatorBase[ILoggingEvent] {
  private val badStartsWith = Set(
    "[GET /try-recover/try_recover_no_recover]",
  )
  override def evaluate(event: ILoggingEvent): Boolean = {
    val msg = event.getFormattedMessage
    msg != null && badStartsWith.exists(msg.startsWith)
  }
}
