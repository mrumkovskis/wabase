package org.wabase.msg

import com.typesafe.config.Config
import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, CoordinatedShutdown}
import org.wabase.{ComponentConf, ComponentConfs, Loggable, invokeFunction}

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Configured messaging providers by name. */
case class WabaseMsgProviders(providers: Map[String, MsgProvider]) {
  /** Returns provider by name, or default provider 'main' if name is null. */
  def provider(name: String): MsgProvider = {
    val n = Option(name).getOrElse(MsgProviderConfig.defaultProviderName)
    providers.getOrElse(n, sys.error(s"Messaging provider '$n' not found. " +
      s"Available names: [${providers.keys.toSeq.sorted.mkString(", ")}]"))
  }

  /** Closes all providers, failure of one provider does not prevent closing others. */
  def close(): Future[Done] = {
    implicit val ec: ExecutionContext = MsgProvider.syncEc
    Future.traverse(providers.toSeq) { case (n, p) =>
      (try p.close() catch { case NonFatal(e) => Future.failed(e) })
        .map(_ => None)
        .recover { case NonFatal(e) => Some(n -> e) }
    }.flatMap { results =>
      val failures = results.flatten
      if (failures.isEmpty) Future.successful(Done)
      else Future.failed(failures.tail.foldLeft(new RuntimeException(
        s"Failed to close messaging provider(s): ${failures.map(_._1).mkString(", ")}", failures.head._2)
      ) { case (ex, (_, e)) => ex.addSuppressed(e); ex })
    }
  }
}

object WabaseMsgProviders extends Loggable {
  /** No providers */
  val empty: WabaseMsgProviders = WabaseMsgProviders(Map.empty[String, MsgProvider])

  /** Creates providers from configuration (see [[MsgProviderConfig]]) and registers their closing
    * in coordinated shutdown phase `service-stop`.
    */
  def apply(system: ActorSystem): WabaseMsgProviders =
    apply(MsgProviderConfig.configs, system)

  def apply(configs: Map[String, Config], system: ActorSystem): WabaseMsgProviders = {
    val providers = WabaseMsgProviders(configs.map { case (n, cfg) => n -> createProvider(n, cfg, system) })
    if (providers.providers.nonEmpty)
      CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseServiceStop, "wabase-msg-providers-close") { () =>
        logger.debug(s"Closing messaging providers: ${providers.providers.keys.toSeq.sorted.mkString(", ")}")
        providers.close()
      }
    providers
  }

  /** Invokes provider factory function [[MsgProviderConfig.factoryFunction]] of class configured in
    * `factory-class` setting. Function can declare any of parameters:
    * `ActorSystem`, `ExecutionContext` (system dispatcher), `Config` (provider config), `String` (provider name).
    */
  def createProvider(name: String, config: Config, system: ActorSystem): MsgProvider = {
    val className = config.getString("factory-class")
    try {
      invokeFunction(className, MsgProviderConfig.factoryFunction, Seq(
        (classOf[ActorSystem],      () => system),
        (classOf[ExecutionContext], () => system.dispatcher),
        (classOf[Config],           () => config),
        (classOf[String],           () => name),
      ))(system.dispatcher) match {
        case p: MsgProvider => p
        case x => sys.error(s"MsgProvider expected but got: '$x' of type ${
          Option(x).map(_.getClass.getName).orNull}")
      }
    } catch {
      case NonFatal(e) => throw new RuntimeException(
        s"Failed to create messaging provider '$name' with $className.${MsgProviderConfig.factoryFunction}: ${
          e.getMessage}", e)
    }
  }
}

/** Configuration of messaging providers. Each object under `msg-provider` is a provider configuration,
  * settings of `msg-provider` (for example `factory-class`) are inherited by providers.
  * Objects named in `msg-provider.common-setting-objects` (for example `codecs`) are settings inherited by providers,
  * not providers.
  */
object MsgProviderConfig {
  val rootPath              = "msg-provider"
  val factoryFunction       = "create"
  val defaultProviderName   = "main"
  val commonSettingObjectsPath = "common-setting-objects"
  lazy val configs: Map[String, Config] = providerConfigs(ComponentConf.getConfigs(rootPath))

  /** Provider configurations, excluding common setting objects. */
  def providerConfigs(confs: ComponentConfs): Map[String, Config] = {
    val common =
      if (confs.root.hasPath(commonSettingObjectsPath))
        confs.root.getStringList(commonSettingObjectsPath).asScala.toSet
      else Set.empty[String]
    confs.confs.filter { case (n, _) => !common.contains(n) }
  }
}
