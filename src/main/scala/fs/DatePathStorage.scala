package org.wabase.fs

import com.typesafe.config.{Config, ConfigException}

import java.time.{Duration, Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import scala.concurrent.Future

abstract class DatePathStorage(cfg: Config) extends Storage {
  protected val Days                  = "days"
  protected val Hours                 = "hours"
  protected val Minutes               = "minutes"
  protected val root: String          = Storage.root(cfg)
  private val formatAndPeriod: (String, Duration) = cfg.getString("files.path-format") match {
    case Days     => ("yyyy/MM/dd",       Duration.ofDays(1))
    case Hours    => ("yyyy/MM/dd/HH",    Duration.ofHours(1))
    case Minutes  => ("yyyy/MM/dd/HH/mm", Duration.ofMinutes(1))
    case x => throw new ConfigException.BadValue(cfg.getValue("files.path-format").origin, "files.path-format",
      s"Invalid path format '$x', must be one of: $Days, $Hours, $Minutes")
  }
  protected val pathFormat: String    = formatAndPeriod._1
  /**
   * Period of stored file path (day, hour or minute according to files.path-format).
   * File cleanup min-age must not be shorter than this, see [[Storage.trashAged]] and
   * [[org.wabase.FileCleanup.checkMinAge]].
   */
  val pathPeriod: Duration            = formatAndPeriod._2
  private val pathFormatter           = DateTimeFormatter.ofPattern(pathFormat).withZone(ZoneOffset.UTC)
  /**
   * Path (relative to root, without file name) where file stored now is placed.
   * UTC based, so that path period is constant - local time daylight saving shift would make it longer.
   */
  protected def currentTailPath: String = pathFormatter.format(Instant.now)
  protected val Trash                 = "trash"

  /** Whether name matches stored file layout yyyy/MM/dd[/HH[/mm]]/sha256 of any path format */
  protected def isStoredName(fullName: String): Boolean =
    DatePathStorage.StoredName.pattern.matcher(fullName).matches
  protected def isAged(lastModifiedMillis: Long, minAge: Duration): Boolean =
    System.currentTimeMillis > lastModifiedMillis + minAge.toMillis
  protected def checkStoredName(fullName: String): Future[Unit] =
    if (isStoredName(fullName)) Future.successful(())
    else Future.failed(new IllegalArgumentException(
      s"File name '$fullName' does not match stored file layout yyyy/MM/dd[/HH[/mm]]/sha256"))
}

object DatePathStorage {
  val StoredName = """\d{4}/\d\d(?:/\d\d){1,3}/[0-9a-fA-F]{64}""".r
}
