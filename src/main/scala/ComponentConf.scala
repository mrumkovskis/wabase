package org.wabase

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigResolveOptions, ConfigValueType}

import scala.jdk.CollectionConverters._

case class ComponentConfs(
  root: Config,
  children: Seq[(String, Config)],
)

trait ComponentConf {
  def getConfigs(
    parentConfPath: String,
    dedicatedConfResourceName: String = null,
    tunablePaths: Set[String] = null,
  ): ComponentConfs
}

object ComponentConf extends ComponentConf {

  private lazy val defaultOverrides           = ConfigFactory.defaultOverrides()
  private lazy val defaultApplication         = ConfigFactory.defaultApplication()
  private lazy val defaultReferenceUnresolved = ConfigFactory.defaultReferenceUnresolved()

  private val delegateClassSetting = "conf-loader-class"
  private lazy val delegate: ComponentConf =
    Option(defaultGetConfigs("component-conf", null, Set.empty).root)
      .filter(_.hasPath(delegateClassSetting))
      .map(getObjectOrNewInstance[ComponentConf](_, delegateClassSetting, "component configuration loader"))
      .orNull

  /**
   * For default implementation, settings for child conf are prioritized over settings from parent conf.
   * 1. Tunable settings from props and confs (application.*, [dedicated conf], reference.conf)
   * 2. Settings in [dedicated conf] (if resource with requested name is in classpath)
   * 3. Settings in props and application confs (application.*, reference.conf)
   */
  def getConfigs(
    parentConfPath: String,
    dedicatedConfResourceName: String = null,
    tunablePaths: Set[String] = null,
  ): ComponentConfs = {
    if (delegate == null || delegate == this)
           defaultGetConfigs(parentConfPath, dedicatedConfResourceName, tunablePaths)
    else delegate.getConfigs(parentConfPath, dedicatedConfResourceName, tunablePaths)
  }

  private def defaultGetConfigs(
    parentConfPath: String,
    dedicatedConfResourceName: String,
    tunablePaths: Set[String],
  ): ComponentConfs = {

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
    val dedicCfgR =
      if (dedicConf.hasPath(r_path)) dedicConf.getConfig(r_path) else ConfigFactory.empty
    val dedicCfgRxT = excludeTunable(dedicCfgR)
    val childConfsRoot = if (dedicConf.isEmpty) tunedCfgR else dedicCfgR

    ComponentConfs(
     dedicCfgRxT.withFallback(tunedCfgR),
     childConfsRoot.root().asScala
      .collect { case (n, v) if v.valueType() == ConfigValueType.OBJECT =>
        val childConf =
          excludeTunable(v.asInstanceOf[ConfigObject].toConfig)
            .withFallback(dedicCfgRxT)
            .withFallback(tunedCfgR.getConfig(n).withFallback(tunedCfgR))
        n -> childConf
      }.toSeq
    )
  }
}
