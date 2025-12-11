package org

import java.util.concurrent.TimeUnit.MILLISECONDS
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.tresql.SimpleCacheBase

import java.lang.reflect.{Constructor, InvocationTargetException, Parameter}
import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

package object wabase extends Loggable {

  import scala.language.existentials
  import scala.language.implicitConversions
  import scala.language.postfixOps
  import scala.language.reflectiveCalls
  import com.typesafe.config._

  lazy val config = ConfigFactory.load

  type jBoolean = java.lang.Boolean
  type jLong = java.lang.Long
  type jDate = java.util.Date
  type sDate = java.sql.Date
  type Timestamp = java.sql.Timestamp
  val TRUE = java.lang.Boolean.TRUE
  val FALSE = java.lang.Boolean.FALSE
  def currentTime = System.currentTimeMillis

  type AppConfig = AppBase.AppConfig
  type AppMdConventions = AppMetadata.AppMdConventions
  type AppVersion = AppServiceBase.AppVersion
  type DbDeferredStorage = DeferredControl.DbDeferredStorage
  type PostgreSqlConstraintMessage = DbConstraintMessage.PostgreSqlConstraintMessage

  type ConstantQueryTimeout = AppServiceBase.ConstantQueryTimeout
  type DefaultAppMdConventions = AppMetadata.DefaultAppMdConventions
  type DefaultAppExceptionHandler[User] = AppServiceBase.AppExceptionHandler.DefaultAppExceptionHandler[User]
  type DefaultServerStatistics = ServerStatistics.DefaultServerStatistics
  type DefaultWsInitialEventsPublisher = ServerNotifications.DefaultInitialEventsPublisher

  type NoAudit[User] = Audit.NoAudit[User]
  type NoCustomConstraintMessage = DbConstraintMessage.NoCustomConstraintMessage
  type NoServerStatistics = ServerStatistics.NoServerStatistics
  type NoWsInitialEvents = ServerNotifications.NoInitialEvents

  type LdapAuthentication = Authentication.LdapAuthentication
  type SimpleExceptionHandler = AppServiceBase.AppExceptionHandler.SimpleExceptionHandler
  type Statistics = ServerStatistics.Statistics

  type InvocationParameter = (Parameter, Int)
  type InvocationParameterFun = PartialFunction[InvocationParameter, Any]

  implicit def toFiniteDuration(d: java.time.Duration): FiniteDuration = Duration.fromNanos(d.toNanos)

  /** Timeout is wrapped into case class so it can be used as implicit parameter */
  case class QueryTimeout(timeoutSeconds: Int)

  /** Default query timeout based on "jdbc.query-timeout" configuration setting */
  lazy val DefaultQueryTimeout: QueryTimeout =
    QueryTimeout(config.getDuration("jdbc.query-timeout").toSeconds.toInt)

  class FunctionInvocationCache(maxSize: Int)
    extends SimpleCacheBase[(Object, java.lang.reflect.Method)](maxSize, "function-invocation-cache")

  private[wabase] lazy val functionInvocationCache =
    new FunctionInvocationCache(config.getInt("app.function-invocation-cache-size"))

  //db connection pool configuration
  def createConnectionPool(config: Config): HikariDataSource = {
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

  def getObjectOrNewInstance[T](cfg: Config, configPath: String, description: String)(implicit m: Manifest[T]): T =
      getObjectOrNewInstance(cfg, configPath, description, Seq.empty, Seq.empty)
  def getObjectOrNewInstance[T](cfg: Config, configPath: String, description: String, potentialParameters: Seq[Any])(implicit m: Manifest[T]): T =
      getObjectOrNewInstance(cfg, configPath, description, potentialParameters, potentialParameters.map(_.getClass))
  def getObjectOrNewInstance[T](cfg: Config, configPath: String, description: String, potentialParameters: Seq[Any], parameterClasses: Seq[Class[_]])(implicit m: Manifest[T]): T = try {
    val className = cfg.getString(configPath)
    val r = getObjectOrNewInstance(className, description, potentialParameters, parameterClasses)
    if (m >:> Manifest.classType(r.getClass))
      r.asInstanceOf[T]
    else
      sys.error(s"Incompatible class ${r.getClass.getName}, expecting $m")
  } catch {
    case util.control.NonFatal(ex) =>
      throw new RuntimeException(s"Failed to get $description instance, please cofigure $configPath properly: ${ex.getMessage}", ex)
  }

  def getObjectOrNewInstance(className: String, description: String): AnyRef =
      getObjectOrNewInstance(className, description, Seq.empty, Seq.empty)
  def getObjectOrNewInstance(className: String, description: String, potentialParameters: Seq[Any]): AnyRef =
      getObjectOrNewInstance(className, description, potentialParameters, potentialParameters.map(_.getClass))
  def getObjectOrNewInstance(className: String, description: String, potentialParameters: Seq[Any], parameterClasses: Seq[Class[_]]): AnyRef = {
    def obj_or_new(cn: String): AnyRef =
      if (cn endsWith "$")
        getObjectOrNewInstance(Class.forName(cn), description, potentialParameters, parameterClasses)
      else try  getNewInstance(Class.forName(cn), description, potentialParameters, parameterClasses) catch {
        case util.control.NonFatal(ex1) =>
          try Class.forName(cn + "$").getField("MODULE$").get(null) catch {
            case util.control.NonFatal(ex2) =>
              val idx = cn.lastIndexOf('.')
              if (idx == -1) {
                logger.debug(s"Failed to get $description instance of class $className, tried both empty constructor and object", ex2)
                throw new RuntimeException(s"Failed to get $description instance of class $className", ex1)
              } else obj_or_new(cn.substring(0, idx) + "$" + cn.substring(idx + 1, cn.length))
          }
        }
    require(className != null, "Class name cannot be null, cannot instantiate class")
    obj_or_new(className)
  }

  def getObjectOrNewInstance(clazz: Class[_], description: String): AnyRef =
      getObjectOrNewInstance(clazz, description, Seq.empty, Seq.empty)
  def getObjectOrNewInstance(clazz: Class[_], description: String, potentialParameters: Seq[Any]): AnyRef =
      getObjectOrNewInstance(clazz, description, potentialParameters, potentialParameters.map(_.getClass))
  def getObjectOrNewInstance(clazz: Class[_], description: String, potentialParameters: Seq[Any], parameterClasses: Seq[Class[_]]): AnyRef = {
    try clazz.getField("MODULE$").get(null) catch {
      case util.control.NonFatal(ex1) =>
        try getNewInstance(clazz, description, potentialParameters, parameterClasses) catch {
          case util.control.NonFatal(ex2) =>
            logger.debug(s"Failed to get $description instance, tried both object and empty constructor", ex1)
            throw new RuntimeException(s"Failed to get $description instance", ex2)
        }
    }
  }

  def getNewInstance(clazz: Class[_], description: String, potentialParameters: Seq[Any], parameterClasses: Seq[Class[_]]): AnyRef = {
    require(potentialParameters.length == parameterClasses.length, "Potential parameters and their classes must have the same length")
    try {
      if (potentialParameters.isEmpty) {
        clazz.getConstructor().newInstance().asInstanceOf[AnyRef]
      } else {
        val paramTypes: Array[Class[_]] = parameterClasses.toArray
        val params: Array[Any] = potentialParameters.toArray
        val ctors: Array[Constructor[_]] = clazz.getConstructors
        var maxLen = -1
        var bestCtor: Constructor[_] = null
        for (c <- ctors) {
          val cTypes: Array[Class[_]] = c.getParameterTypes
          val len: Int = cTypes.length
          def assignableMatch = (0 until len).forall(i => cTypes(i).isAssignableFrom(paramTypes(i)))
          if (len <= paramTypes.length && assignableMatch) {
            if (len > maxLen) {
              maxLen = len
              bestCtor = c
            }
          }
        }
        if (bestCtor != null) {
          bestCtor.newInstance(params.take(maxLen).map(_.asInstanceOf[AnyRef]): _*).asInstanceOf[AnyRef]
        } else {
          throw new NoSuchMethodException(
            s"No suitable constructor found for class ${clazz.getName}, parameter classes: [${parameterClasses.map(_.getName).mkString(", ")}]")
        }
      }
    } catch {
      case util.control.NonFatal(ex) =>
        throw new RuntimeException(s"Failed to get $description instance", ex)
    }
  }

  def getObjAndFunction(className: String, function: String): (AnyRef, java.lang.reflect.Method) =
    functionInvocationCache.get(s"$className.$function").getOrElse {
      val obj = getObjectOrNewInstance(className, s"function $function")
      val clazz = obj.getClass
      def objAndFun(m: java.lang.reflect.Method) = {
        val obj_fun = (obj, m)
        functionInvocationCache.put(s"$className.$function", obj_fun)
        obj_fun
      }
      clazz.getMethods.filter(m => m.getName == function) match {
        case Array(method) => objAndFun(method)
        case Array() =>
          logger.debug(s"Method $function not found in class $className")
          sys.error(s"Method $function not found in class $className")
        case m =>
          logger.debug(s"Multiple methods '$function' found: (${m.toList}) in class $className")
          sys.error(s"Multiple methods '$function' found: (${m.toList}) in class $className")
      }
    }

  def invocationParameter(parameters: Seq[(Class[_], () => Any)])(parameter: InvocationParameter): Any = {
    val (par, _) = parameter
    val parameterClass = par.getType
    parameters.collectFirst {
      case (c, f) if parameterClass.isAssignableFrom(c) => f()
    }.getOrElse(throw new IllegalArgumentException(s"Cannot find value for function parameter '${
      par.getName}: ${parameterClass.getName}'.\nAvailable parameters are of type: (${
        parameters.map(_._1.getName).mkString(", ")
      })"))
  }

  def invokeFunction(
    className: String,
    function: String, getParameter: InvocationParameter => Any,
  )(implicit ec: ExecutionContext): Any = {
    def getParams(m: java.lang.reflect.Method) = {
      val params = m.getParameters.zipWithIndex map { case (pt, idx) => getParameter(pt -> idx) -> pt }
      if (params.exists { case (v, p) => v.isInstanceOf[Future[_]] && !classOf[Future[_]].isAssignableFrom(p.getType) })
        Future.traverse(params.toSeq) {
          case (f: Future[_], p) if !classOf[Future[_]].isAssignableFrom(p.getType) => f
          case (x, _) => Future.successful(x)
        }.map(_.toArray) else params.map(_._1)
    }
    def call(o: Object, m: java.lang.reflect.Method) =
      try {
        getParams(m) match {
          case fp: Future[_] => fp.flatMap { pars =>
            m.invoke(o, pars.asInstanceOf[Array[Object]]: _*) match {
              case f: Future[_] => f
              case v => Future.successful(v)
            }
          }.recoverWith {
            // unwrap invocation target exception
            case e: InvocationTargetException if e.getCause != null => Future.failed(e.getCause)
          }
          case p => m.invoke(o, p.asInstanceOf[Array[Object]]: _*)
        }
      } // cast is needed for scala 2.12.x
      catch {
        // unwrap invocation target exception
        case e: InvocationTargetException if e.getCause != null => throw e.getCause
      }
    val (obj, method) = getObjAndFunction(className, function)
    call(obj, method)
  }

  def invokeFunction(
     className: String,
     function: String,
     parameters: Seq[(Class[_], () => Any)],
  )(implicit ec: ExecutionContext): Any =
    invokeFunction(className, function, invocationParameter(parameters)(_))

  def invokeFunction(
    className: String,
    function: String,
    fallbackParameters: Seq[(Class[_], () => Any)],
    parameterFun: InvocationParameterFun,
  )(implicit ec: ExecutionContext): Any = {
    val default: InvocationParameterFun = {
      case p => invocationParameter(fallbackParameters)(p)
    }
    invokeFunction(className, function, parameterFun orElse default)
  }

  case class PoolName(connectionPoolName: String)
  lazy val DEFAULT_CP = {
    val dcp = PoolName(TresqlResourcesConf.DefaultCpName)
    if (dcp.connectionPoolName == null)
      logger.debug("Default JDBC connection pool disabled")
    else if (!config.hasPath(s"jdbc.cp.${dcp.connectionPoolName}"))
      logger.warn(s"Default JDBC connection pool configuration missing (key jdbc.cp.${dcp.connectionPoolName}).")
    dcp
  }

  object ConnectionPools {
    private lazy val cps = {
      val c = config.getConfig("jdbc.cp")
      val s: Seq[(PoolName, DataSource)] =
        c.root().asScala.keys.map(v => (PoolName(v), createConnPool(c.getConfig(v)))).toSeq ++
          Seq(PoolName(null) -> DisabledDataSource)
      scala.collection.concurrent.TrieMap(s: _*)
    }

    private def createConnPool(conf: Config) = try createConnectionPool(conf) catch {
      case NonFatal(ex) =>
        logger.error(s"Error initializing Db connection pool for $conf", ex)
        null
    }

    def key(poolName: String): PoolName =
      if (poolName != null) PoolName(poolName) else DEFAULT_CP

    def apply(poolName: String): DataSource =
      apply(key(poolName))
    def apply(pool: PoolName): DataSource = {
      val ds = cps.getOrElse(pool, {
        require(pool == null || pool.connectionPoolName == null,
          s"""Unable to find connection pool "${pool.connectionPoolName}"""")
        cps(DEFAULT_CP)
      })
      if (ds != null) ds
      else apply(pool, () => createConnectionPool(config.getConfig(s"jdbc.cp.${pool.connectionPoolName}")))
    }

    def apply(pool: PoolName, factoryFun: () => DataSource): DataSource = {
      cps.getOrElse(pool, {
        val ds = factoryFun()
        cps.put(pool, ds)
        ds
      })
    }
  }

  object DisabledDataSource extends DataSource {
    // Members declared in javax.sql.CommonDataSource
    override def getParentLogger(): java.util.logging.Logger = ???

    // Members declared in javax.sql.DataSource
    override def getConnection(username: String, password: String): java.sql.Connection = null
    override def getConnection(): java.sql.Connection = null
    override def getLogWriter(): java.io.PrintWriter = ???
    override def getLoginTimeout(): Int = ???
    override def setLogWriter(out: java.io.PrintWriter): Unit = ???
    override def setLoginTimeout(seconds: Int): Unit = ???

    // Members declared in java.sql.Wrapper
    override def isWrapperFor(iface: Class[_]): Boolean = false
    override def unwrap[T](iface: Class[T]): T = ???
  }
}
