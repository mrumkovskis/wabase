package org.wabase

import com.typesafe.scalalogging.{Logger, LazyLogging}
import org.slf4j.{LoggerFactory, Marker}

trait Loggable{
  protected lazy val logger: Logger = {
    val name = getClass.getName
    val nameUpdated = if (name.endsWith("$")) name.substring(0, name.length - 1) else name
    Logger(LoggerFactory.getLogger(nameUpdated))
  }
}
