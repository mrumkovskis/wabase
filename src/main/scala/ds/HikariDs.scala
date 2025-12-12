package org.wabase.ds

import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import scala.jdk.CollectionConverters._

import java.util.concurrent.TimeUnit.MILLISECONDS

object HikariDs {
  def create(config: Config): HikariDataSource = {
    val props = new java.util.Properties(System.getProperties)
    for (e <- config.entrySet.asScala) {
      val key = e.getKey
      if (key.toLowerCase.contains("time") || key == "leakDetectionThreshold")
        props.setProperty(key, "" + config.getDuration(key, MILLISECONDS))
      else
        props.setProperty(key, config.getString(key))
    }
    val hikariConfig = new HikariConfig(props)
    new HikariDataSource(hikariConfig)
  }
}
