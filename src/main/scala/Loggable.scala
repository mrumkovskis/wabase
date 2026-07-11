package org.wabase

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait Loggable {
  def loggerName: String = {
    val name = getClass.getName
    if (name.endsWith("$")) name.substring(0, name.length - 1) else name
  }
  protected lazy val logger: Logger = Logger(LoggerFactory.getLogger(loggerName))
}
