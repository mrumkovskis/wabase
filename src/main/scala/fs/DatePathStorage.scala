package org.wabase.fs

import com.typesafe.config.{Config, ConfigException}

abstract class DatePathStorage(cfg: Config) extends Storage {
  protected val Days                  = "days"
  protected val Hours                 = "hours"
  protected val Minutes               = "minutes"
  protected val root: String          = Storage.root(cfg)
  protected val pathFormat: String    = cfg.getString("files.path-format") match {
    case Days     => "yyyy/MM/dd"
    case Hours    => "yyyy/MM/dd/HH"
    case Minutes  => "yyyy/MM/dd/HH/mm"
    case x => throw new ConfigException.BadValue(cfg.getValue("files.path-format").origin, "files.path-format",
      s"Invalid path format '$x', must be one of: $Days, $Hours, $Minutes")
  }
}
