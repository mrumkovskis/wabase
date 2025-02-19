package org.wabase

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigResolveOptions, ConfigValueType}

import scala.jdk.CollectionConverters._

object ComponentConf {

  private lazy val defaultOverrides           = ConfigFactory.defaultOverrides()
  private lazy val defaultApplication         = ConfigFactory.defaultApplication()
  private lazy val defaultReferenceUnresolved = ConfigFactory.defaultReferenceUnresolved()

  /**
   * Settings for child conf are prioritized over settings from parent conf.
   * 1. Tunable settings from props and confs (application.*, [dedicated conf], reference.conf)
   * 2. Settings in [dedicated conf] (if resource with requested name is in classpath)
   * 3. Settings in props and application confs (application.*, reference.conf)
   */
  def getConfigs(
    parentConfPath: String,
    dedicatedConfResourceName: String = null,
    tunablePaths: Set[String] = null,
  ): Seq[(String, Config)] = {

    val dedicLoad = ConfigFactory.parseResources(Option(dedicatedConfResourceName).getOrElse(s"$parentConfPath.conf"))
    val dedicConf = dedicLoad.resolve(ConfigResolveOptions.noSystem())

    val tunedConf = defaultOverrides
        .withFallback(defaultApplication)
        .withFallback(dedicLoad)
        .withFallback(defaultReferenceUnresolved)
        .resolve()

    /* Removes tunable tuned (i.e. found in tuned conf) paths from dedicated conf */
    def excludeTunable(dedicConf: Config): Config =
      if (tunablePaths == null) ConfigFactory.empty else tunablePaths.foldLeft(dedicConf)(_ withoutPath _)

    val r_path    = parentConfPath
    val tunedCfgR =
      if (tunedConf.hasPath(r_path)) tunedConf.getConfig(r_path) else ConfigFactory.empty
    val dedicCfgR = excludeTunable(
      if (dedicConf.hasPath(r_path)) dedicConf.getConfig(r_path) else ConfigFactory.empty
    )

    tunedCfgR.root().asScala
      .collect { case (n, v) if v.valueType() == ConfigValueType.OBJECT =>
        val childConf =
          excludeTunable(v.asInstanceOf[ConfigObject].toConfig)
            .withFallback(dedicCfgR)
            .withFallback(tunedCfgR.getConfig(n).withFallback(tunedCfgR))
        n -> childConf
      }.toSeq
  }
}
