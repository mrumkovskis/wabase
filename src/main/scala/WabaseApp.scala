package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.model.headers.`Timeout-Access`
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.mojoz.metadata.{FieldDef, ViewDef}
import org.mojoz.querease.TresqlMetadata
import org.tresql.SingleValueResult
import org.wabase.AppMetadata.{Action, AugmentedAppFieldDef, AugmentedAppViewDef}
import org.wabase.AppMetadata.Action.{LimitKey, OffsetKey, OrderKey}
import org.wabase.AppQuerease.InjectionParametersContext
import org.wabase.client.HttpClientConfig

import java.util.Locale
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}

case class WabaseHttpClients(httpClients: Map[String, InjectionParametersContext => HttpRequest => Future[HttpResponse]])
case class WabaseFileStreamers(fileStreamers: Map[String, FileStreamer]) {
  def fs(name: String): FileStreamer = {
    Option(name).map { n =>
      fileStreamers.getOrElse(n, sys.error(s"Filestreamer '$n' not found. " +
        s"Available names: [${fileStreamers.keys.toSeq.sorted.mkString(", ")}]"))
    }.getOrElse {
      fileStreamers.getOrElse("main", null)
    }
  }
}

trait WabaseApp[User] {
  this:  AppBase[User]
    with Audit[User]
    with DbAccess
    with Authorization[User]
    with DbConstraintMessage
    =>

  import qe.viewDefOption

  val SerializationBufferSize: Int = WabaseAppConfig.SerializationBufferSize
  val SerializationBufferMaxFileSize: Long = WabaseAppConfig.SerializationBufferMaxFileSize
  val SerializationBufferMaxFileSizes: Map[String, Long] = WabaseAppConfig.SerializationBufferMaxFileSizes

  def tresqlMetadata: TresqlMetadata = qe.tresqlMetadata

  def viewSerializationBufferMaxFileSize(viewName: String): Long =
    SerializationBufferMaxFileSizes.getOrElse(viewName, SerializationBufferMaxFileSize)

  type ActionHandlerResult = qe.QuereaseAction[WabaseResult]
  type ActionHandler       = AppActionContext => ActionHandlerResult
  implicit lazy val httpClients: WabaseHttpClients =
    WabaseHttpClients(HttpClientConfig.httpClientFactory.createHttpClients)
  implicit lazy val fileStreamers: WabaseFileStreamers =
    WabaseFileStreamers(FileStreamerConfig.fileStreamerFactory.createFileStreamers(this))
  lazy val injectionParametersProvider: AppQuerease.InjectionParametersProvider =
    AppQuerease.injectionParametersProviderFactory.createInjectionParametersProvider

  case class AppActionContext(
    actionName: String,
    viewName:   String,
    keyValues:  Seq[Any],
    params:     Map[String, Any],
    values:     Map[String, Any],
    resultFilter: ResultRenderer.ResultFilter = null,
    oldValue:   Map[String, Any] = null, // for save and delete
    serializedResult: Source[ByteString, _] = null,
  )(implicit
    val user:     User,
    val state:    ApplicationState,
    val ec:       ExecutionContext,
    val as:       ActorSystem,
    val rf:       ResourcesFactory,
    val httpReq:  HttpRequest,
  ) {
    lazy val env: Map[String, Any] = state ++
      Option(httpReq)
        .flatMap(WabaseAuthentication.session)
        .map(sid => Seq(WabaseAuthentication.SessionCookieName -> sid))
        .getOrElse(Nil) ++ current_user_param(user)
    def withResultFilter(resFil: ResultRenderer.ResultFilter): AppActionContext =
      copy(resultFilter = resFil)
  }

  case class WabaseResult(ctx: AppActionContext, result: QuereaseResult)

  /** Override to request serialized result source in AppActionContext, e.g., for auditing */
  protected def shouldAddResultToContext(context: AppActionContext): Boolean = false

  def doWabaseAction(
    actionName: String,
    viewName:   String,
    keyValues:  Seq[Any],
    params:     Map[String, Any],
    values:     Map[String, Any] = Map(),
    resultFilter: ResultRenderer.ResultFilter = null,
    doApiCheck: Boolean = true,
  )(implicit
    user:     User,
    state:    ApplicationState,
    ec:       ExecutionContext,
    as:       ActorSystem,
    qt:       QueryTimeout,
    httpReq:  HttpRequest,
  ): Future[WabaseResult] = {
    val vdo = qe.viewDefOption(viewName)
    val rf = resourceFactory(viewName, actionName, qt)
    doWabaseAction(
      AppActionContext(actionName, viewName, keyValues, params, values ++ params, resultFilter)(
        user, state, ec, as, rf, setMaxContentSize(setTimeout(httpReq, vdo), vdo)),
      doApiCheck)
  }

  def doAction(
    actionName: String,
    viewName:   String,
    keyValues:  Seq[Any],
    params:     Map[String, Any],
    values:     Map[String, Any] = Map(),
    resultFilter: ResultRenderer.ResultFilter = null,
    doApiCheck: Boolean = true,
  )(implicit wrctx: WabaseRequestContext): Future[WabaseResult] = {
    val vdo = qe.viewDefOption(viewName)
    val rf = resourceFactory(viewName, actionName, wrctx.queryTimeout)
    doWabaseAction(
      AppActionContext(actionName, viewName, keyValues, params, values ++ params, resultFilter)(
        wrctx.user.asInstanceOf[User], wrctx.applicationState, wrctx.as.dispatcher, wrctx.as, rf, wrctx.req),
      doApiCheck)
  }

  def _api(implicit user: User) = api
  def _apiMetadata(implicit user: User, state: ApplicationState) = apiMetadata
  def _metadata(viewName: String)(implicit user: User, state: ApplicationState) = metadata(viewName)

  private def setMaxContentSize(httpReq: HttpRequest, vdo: Option[ViewDef]) = vdo.map { vd =>
    if (vd.maxContentSize == null || httpReq == null) httpReq
    else httpReq.withEntity(httpReq.entity.withSizeLimit(vd.maxContentSize))
  }.getOrElse(httpReq)

  private def setTimeout(httpReq: HttpRequest, vdo: Option[ViewDef]) = vdo.map { vd =>
    if(vd.timeout == null || httpReq == null) httpReq
    else {
      httpReq.header[`Timeout-Access`].map(_.timeoutAccess.updateTimeout(vd.timeout))
        .getOrElse(logger.warn(s"request timeout is defined for view ${vd.name}, however no request-timeout http header is set!"))
      httpReq
    }
  }.getOrElse(httpReq)

  protected def doWabaseAction(
    context:    AppActionContext,
    doApiCheck: Boolean,
  ): Future[WabaseResult] =
    doWabaseAction(getActionHandler(context), context, doApiCheck)

  protected def doWabaseAction(
    action:     ActionHandler,
    context:    AppActionContext,
    doApiCheck: Boolean,
  ): Future[WabaseResult] = {
    val actionContext = beforeWabaseAction(context, doApiCheck)
    import context.ec
    import context.as
    action(actionContext)
      .run(ec, as)
      .flatMap(maybeSerializeResult(context, _))
      .andThen {
        case Success(WabaseResult(ctx, res)) => this.afterWabaseAction(ctx, Success(res))
        case Failure(error) => this.afterWabaseAction(context, Failure[QuereaseResult](error))
      }
  }

  def simpleAction(context: AppActionContext): ActionHandlerResult = {
    import context._
    qe.QuereaseAction(viewName, actionName, values, env, context.resultFilter)(
        rf, httpReq, qio, fileStreamers, httpClients, injectionParametersProvider)
      .map(WabaseResult(context, _))
  }

  def list(context: AppActionContext): ActionHandlerResult = {
    import context._
    val offset  = values.get(OffsetKey).map(_.toString.toInt) getOrElse 0
    val limit   = values.get(LimitKey ).map(_.toString.toInt) getOrElse 0
    val orderBy = values.get(OrderKey ).map(_.toString).orNull
    val viewDef = qe.viewDef(viewName)
    checkLimit(viewDef, limit)
    checkOffset(viewDef, offset)
    checkOrderBy(viewDef, orderBy)
    val forcedLimit =
      Option(limit).filter(_ > 0) getOrElse Option(viewDef.limit).filter(_ > 0).map(_ + 1).getOrElse(0)
    val trustedLimitOffsetOrderBy = Map(
      OffsetKey -> offset,
      LimitKey  -> forcedLimit,
      OrderKey  -> stableOrderBy(viewDef, orderBy),
    ).filterNot(_._2 == null)
    val trusted = values ++ trustedLimitOffsetOrderBy ++ current_user_param(user)
    simpleAction(context.copy(values = trusted))
  }

  protected def maybeGetOldValue(context: AppActionContext): qe.QuereaseAction[Map[String, Any]] = {
    qe.QuereaseAction.value(context.values)
  }

  /** In subclass can place this method call in {{{maybeGetOldValue}}} method */
  protected def getOldValue(context: AppActionContext): qe.QuereaseAction[Map[String, Any]] = {
    import context._
    def throwUnexpectedResultClass(qr: QuereaseResult) =
      sys.error(s"Unexpected result class getting old-value for '$actionName' of $viewName: ${qr.getClass.getName}")
    def oldVal(ov: QuereaseResult): Map[String, Any] = ov match {
      case ResponseResult(StatusCodes.NotFound.intValue, _, _, _) => null
      case MapResult(oldMap) => oldMap
      case srr: TresqlSingleRowResult => srr.map(qe.toCompatibleMap(_, qe.viewDef(viewName)))
      case CompatibleResult(r, _, _) => oldVal(r)
      case qrwc: QuereaseResultWithCleanup => qrwc map oldVal
      case x => throwUnexpectedResultClass(x)

    }
    qe.QuereaseAction(viewName, Action.Get, values, env,
      context.resultFilter)(rf, httpReq, qio, fileStreamers, httpClients, injectionParametersProvider).map(oldVal)
  }
  protected def throwOldValueNotFound(message: String, locale: Locale): Nothing =
    throw new org.mojoz.querease.NotFoundException(translate(message)(locale))

  def save(context: AppActionContext): ActionHandlerResult = {
    import context._
    val viewDef = qe.viewDef(viewName)
    val keyAsMap =
      if  (actionName == Action.Update)
           values.getOrElse(qe.oldKeyParamName, Map.empty: Map[String, Any]) match {
             case keyAsMap: Map[String, Any] @unchecked => keyAsMap
             case x => sys.error("Unexpected old key type, expecting Map")
           }
      else prepareKey(viewName, keyValues, actionName)
    Option(keyAsMap)
      .filter(_.nonEmpty)
      .map(_ => maybeGetOldValue(context.copy(values = keyAsMap)))
      .getOrElse(qe.QuereaseAction.value(null: Map[String, Any]))
      .flatMap { oldValue =>
        val richContext = context.copy(oldValue = oldValue)
        val saveable = applyReadonlyValues(viewDef, oldValue, values)
        val saveableContext = richContext.copy(values = saveable)
        validateFields(viewName, saveable)
        this.scriptValidations(saveableContext)(state.locale)
        qe.QuereaseAction(viewName, context.actionName, saveable, env, context.resultFilter)(rf,
            httpReq, qio, fileStreamers, httpClients, injectionParametersProvider)
          .map(WabaseResult(saveableContext, _))
          .recover { case ex => friendlyConstraintErrorMessage(viewDef, throw ex)(state.locale) }
      }
  }

  def delete(context: AppActionContext): ActionHandlerResult = {
    import context._
    maybeGetOldValue(context).flatMap { oldValue =>
      val richContext = context.copy(oldValue = oldValue)
      qe.QuereaseAction(viewName, actionName, values, env, context.resultFilter)(
          rf, httpReq, qio, fileStreamers, httpClients, injectionParametersProvider)
        .map(WabaseResult(richContext, _))
        .recover { case ex => friendlyConstraintErrorMessage(throw ex)(state.locale) }
    }
  }

  protected def getActionHandler(context: AppActionContext): ActionHandler = {
    import context._
    actionName match {
      case Action.List    => list
      case Action.Save    => save
      case Action.Insert  => save
      case Action.Update  => save
      case Action.Upsert  => save
      case Action.Delete  => delete
      case x              => simpleAction
    }
  }

  def resourceFactory(viewName: String, actionName: String, qt: QueryTimeout): ResourcesFactory = {
    resourceFactory(viewDefOption(viewName).orNull, s"$actionName", qt)
  }

  def maybeSerializeResult(context: AppActionContext, wr: WabaseResult): Future[WabaseResult] = wr match {
    case wr@WabaseResult(_, sr@ResponseResult(_, ResultValue(r), _, _)) =>
      maybeSerializeResult(context, wr.copy(result = r))
        .map{nwr => nwr.copy(result = sr.copy(value = ResultValue(nwr.result)))}(context.ec)
    case WabaseResult(ac, QuereaseResultWithCleanup(result, cleanup)) =>
      sealed trait Res
      case class SourceRes(src: Source[ByteString, _],
                           filter: ResultRenderer.ResultFilter,
                           isCollection: Boolean) extends Res
      case class StrictRes(result: QuereaseResult) extends Res
      def res(r: QuereaseResult, filter: ResultRenderer.ResultFilter): Res = {
        def single_res(sr: SingleValueResult[_]) = sr.value match {
          case m: Map[String@unchecked, _] =>
            if (filter == ResultRenderer.NoFilter) StrictRes(AnyResult(m))
            else StrictRes(MapResult(m)) // return strict since structure may not conform to tresql result so serialization might fail
          case s: Seq[Map[String, _]@unchecked] => SourceRes(DataSerializer.source(() => s.iterator), filter, true) // serialization might fail if data does not conform to tresql result table structure
          case qr: QuereaseResult => res(qr, filter)
          case x => StrictRes(StringResult(String.valueOf(x)))
        }
        r match {
          case TresqlResult(tr) => tr match {
            case r: SingleValueResult[_] => single_res(r)
            case _ => SourceRes(TresqlResultSerializer.source(() => tr), filter, true)
          }
          case TresqlSingleRowResult(row) => row match {
            case r: SingleValueResult[_] => single_res(r)
            case _ => SourceRes(TresqlResultSerializer.rowSource(() => row), filter, false)
          }
          case IteratorResult(ir) =>
            SourceRes(DataSerializer.source(() => ir), filter, true)
          case CompatibleResult(dr: QuereaseCloseableResult, filter, _) => res(dr, filter)
          case x => StrictRes(x)
        }
      }
      res(result, null) match {
        case SourceRes(resultSource, filter, isCollection) =>
          import context._
          val addResultToContext = shouldAddResultToContext(context)
          ResultSerializer.serializeResult(SerializationBufferSize, viewSerializationBufferMaxFileSize(ac.viewName),
            resultSource, cleanup, if (addResultToContext) 2 else 1)
            .map { srs =>
              val qsr = QuereaseSerializedResult(srs.head, filter, isCollection)
              val ac =
                if (addResultToContext) srs.tail.head match {
                  case CompleteResult(bs) => context.copy(serializedResult = Source.single(bs))
                  case IncompleteResultSource(s) => context.copy(serializedResult = s)
                } else context
              WabaseResult(ac, qsr)
            }
        case StrictRes(strictRes) =>
          cleanup(None)
          Future.successful(WabaseResult(ac, strictRes))
      }
    case wr => Future.successful(wr)
  }

  def checkKeySize(viewDef: ViewDef, keySize: Int, actionName: String): Unit = {
    val expectedKeySize = qe.viewNameToApiKeyFields.get(viewDef.name).map(_.size).getOrElse(-1)
    checkKeySize(viewDef, expectedKeySize, keySize, actionName)
  }

  def checkKeySize(viewDef: ViewDef, expectedKeySize: Int, keySize: Int, actionName: String): Unit = actionName match {
    case Action.List | Action.Count =>
      if (keySize < viewDef.minKeySizeForList || keySize > viewDef.maxKeySizeForList) {
        throw new BusinessException(
          s"Invalid key size for $actionName of '${viewDef.name}'. " +
            s"Expecting ${viewDef.expectedKeySizeDescr}, got $keySize")
      }
    case _ =>
      if (keySize != expectedKeySize) {
        throw new BusinessException(
          s"Invalid key size for $actionName of '${viewDef.name}'. " +
            s"Expecting $expectedKeySize, got $keySize")
      }
  }

  /** Converts key value from uri representation to appropriate type.
    * Default implementation also converts "null" to null.
    */
  def prepareKeyValue(field: FieldDef, value: Any): Any =
    if (value == "null") null else qe.convertToType(value, field.type_)
  def prepareKey(viewName: String, keyValues: Seq[Any], actionName: String): Map[String, Any] = {
        val keyFields = qe.viewNameToApiKeyFields(viewName)
        prepareKey(viewName, keyFields, keyValues, actionName)
  }
  def prepareKey(viewName: String, keyFields: Seq[FieldDef], keyValues: Seq[Any], actionName: String): Map[String, Any] = {
    if (keyValues.nonEmpty) {
        checkKeySize(qe.nameToViewDef(viewName), keyFields.size, keyValues.length, actionName)
        keyFields.zip(keyValues).map { case (f, v) =>
          try f.fieldName -> prepareKeyValue(f, v)
          catch {
            case util.control.NonFatal(ex) => throw new BusinessException(
              s"Failed to convert value for key field ${f.name} to type ${f.type_.name}", ex)
          }
        }.toMap
    } else Map.empty
  }

  private val fieldFilterParameterNameOpt =
    Option("app.field-filter-parameter-name").filter(config.hasPath).map(config.getString)

  protected def addResultFilter(context: AppActionContext): AppActionContext = {
    if (context.resultFilter != null) context
    else context.actionName match {
      case Action.Get | Action.List | Action.Create =>
        val allowed = fieldFilterParameterNameOpt.flatMap(context.params.get).map {
          case null => null
          case seq: Seq[_] => seq.map(_.toString).toSet
          case cols => s"$cols".split(",").map(_.trim).toSet
        }.orNull
        logger.debug(s"Adding result filter. allowed: ${allowed}")
        if (allowed != null) {
          class ColsFilter(viewName: String, nameToViewDef: Map[String, ViewDef])
            extends ResultRenderer.ViewFieldFilter(viewName, nameToViewDef) {
            override def shouldInclude(field: String) =
              allowed.contains(field) && super.shouldInclude(field)
            override def childFilter(field: String) = viewDef.fieldOpt(field)
              .map(_.type_.name)
              .map(new ColsFilter(_, nameToViewDef))
              .orNull
          }
          context.withResultFilter(new ColsFilter(context.viewName, qe.nameToViewDef))
        } else context
      case _ => context
    }
  }

  protected def beforeWabaseAction(
    context:    AppActionContext,
    doApiCheck: Boolean,
  ): AppActionContext = {
    import context._
    if (doApiCheck)
      checkApi(viewName, actionName, user, keyValues)
    val keyAsMap = prepareKey(viewName, keyValues, actionName)
    val key_params =
      if  (context.actionName == Action.Update && keyAsMap != null && keyAsMap.nonEmpty)
           Map(qe.oldKeyParamName -> keyAsMap)
      else keyAsMap
    addResultFilter(
      context.copy(values = values ++ key_params)
    )
  }

  protected def afterWabaseAction(context: AppActionContext, result: Try[QuereaseResult]): Unit = {}

  protected def applyReadonlyValues(
    viewDef: ViewDef, old: Map[String, Any], instance: Map[String, Any]
  ): Map[String, Any] = {
    // overwrite incoming values of non-updatable fields with old values from db
    // FIXME for lookups and children?
    val fieldsToCopy =
      if (old == null) Nil
      else viewDef.fields.filterNot(_.api.updatable)
    if (fieldsToCopy.nonEmpty) {
      val fieldsToCopyNames = fieldsToCopy.map(_.fieldName).toSet
      instance ++ old.filter { case (k, v) => fieldsToCopyNames.contains(k) }
    } else
      instance
  }

  protected def splitOrderBy(orderBy: String): Seq[String] =
    if (orderBy == null || orderBy == "")
      Seq.empty
    else if (orderBy.indexOf('(') < 0)
      orderBy.split("\\s*\\,\\s*").toSeq.filter(_ != "")
    else
      qe.parser.parseWithParser(qe.parser.colAndOrd)(s"#($orderBy)")._2.cols.map(_.tresql)
  protected def extractNamesFromOrderBy(orderBy: String): Seq[String] =
    if (orderBy == null || orderBy == "")
      Seq.empty
    else
      splitOrderBy(orderBy)
        .map(nameFromOrderByPart)
        .filter(_ != "")
  protected def nameFromOrderByPart(orderByPart: String): String =
      orderByPart
         .replaceFirst("^\\s*null\\s+", "")
         .replaceFirst("\\s+null\\s*$", "")
         .replaceFirst("^\\s*~\\s*", "")
         .trim
  protected def stableOrderBy(viewDef: ViewDef, orderBy: String): String = {
    val parts          = substitutedOrderBy(viewDef, orderBy)
    val forcedSortCols = parts.map(nameFromOrderByPart).toSet
    val orderBySubst   = if (orderBy != null) parts.mkString(", ") else null
    if (forcedSortCols.nonEmpty && viewDef.orderBy != null && viewDef.orderBy.nonEmpty) {
      Option(viewDef.orderBy)
        .map(_.filter(c => !forcedSortCols.contains(nameFromOrderByPart(c))))
        .filter(_.nonEmpty)
        .map(s => (orderBySubst :: s.toList).mkString(", "))
        .getOrElse(orderBySubst)
    } else orderBySubst
  }
  protected def substitutedOrderBy(viewDef: ViewDef, orderBy: String): Seq[String] =
    splitOrderBy(orderBy).flatMap { part =>
      viewDef.fieldOpt(nameFromOrderByPart(part))
        .map(_.orderBy)
        .filter(_ != null)
        .map(splitOrderBy)
        .map { substituteParts =>
          val forcedNullsFirst = part.startsWith("null ")
          val forcedNullsLast  = part.endsWith(" null")
          val forcedDesc       = part.replaceFirst("^null\\s+", "").trim.startsWith("~")
          substituteParts.map { substitutePart =>
            val substName       = nameFromOrderByPart(substitutePart)
            val substNullsFirst = substitutePart.startsWith("null ")
            val substNullsLast  = substitutePart.endsWith(" null")
            val substDesc       = substitutePart.replaceFirst("^null\\s+", "").trim.startsWith("~")
            val nullsFirst      = !forcedNullsLast  && (forcedNullsFirst || substNullsFirst)
            val nullsLast       = !forcedNullsFirst && (forcedNullsLast  || substNullsLast )
            val desc            = forcedDesc != substDesc
            s"${if (nullsFirst) "null " else ""}${if (desc) "~" else ""}${substName}${if (nullsLast) " null" else ""}"
          }
        }
        .getOrElse(Seq(part))
    }.filter(_ != "")
  private val ident = "[_\\p{IsLatin}][_\\p{IsLatin}0-9]*"
  private val qualifiedIdent = s"$ident(\\.$ident)*"
  private val qualifiedIdentRegex = s"^$qualifiedIdent$$".r
  private val validViewNameRegex = s"^$qualifiedIdent$$".r
  def sanitizedViewName(viewName: String) =
    if (validViewNameRegex.pattern.matcher(viewName).matches()) viewName else "Strange name"
  protected def noApiException(viewName: String, method: String, user: User): Exception =
    new BusinessException(s"${sanitizedViewName(viewName)}.$method is not a part of this API")
  protected def apiUnauthorizedException(viewName: String, method: String, user: User): Exception =
    if  (user == null)
         new AuthenticationException("Unauthorized")
    else new AuthorizationException("Forbidden")
  def checkApi[F](viewName: String, method: String, user: User, keyValues: Seq[Any]): Unit = {
    (for {
      view <- viewDefOption(viewName)
      roles <- view.apiMethodToRoles.get(method).orElse(method match {
        case Action.Insert |
             Action.Update => view.apiMethodToRoles.get(Action.Save)
        case x => None
      })
      result <-
        if (qe.isPublicView(viewName) || roles.contains(qe.publicApiRoleName) || hasRole(user, roles))
          Option(true)
        else throw apiUnauthorizedException(viewName, method, user)
    } yield result).getOrElse(
      throw noApiException(viewName, method, user)
    )
  }
  protected def checkLimit(viewDef: ViewDef, limit: Int): Unit = {
    val maxLimitForView = viewDef.limit
    if (maxLimitForView > 0 && limit > maxLimitForView)
      throw new BusinessException(
        s"limit $limit exceeds max limit allowed for ${viewDef.name}: $maxLimitForView")
  }
  protected def checkOffset(viewDef: ViewDef, offset: Int): Unit =
    if (offset < 0)
      throw new BusinessException("offset must not be negative")
  protected def checkOrderBy(viewDef: ViewDef, orderBy: String): Unit = {
    if (orderBy != null) {
      val sortableFields =
        viewDef.fields.filter(_.sortable).map(_.fieldName).toSet
      val sortCols = extractNamesFromOrderBy(orderBy)
      val notSortable = sortCols.filterNot(sortableFields.contains)
      def notSortableSafe =
        notSortable.map { sortCol =>
          if  (qualifiedIdentRegex.pattern.matcher(sortCol).matches())
               sortCol
          else "(strange name)"
        }
      if (notSortable.nonEmpty)
        throw new BusinessException(s"Not sortable: ${viewDef.name} by " + notSortableSafe.mkString(", "), null)
    }
  }
  private val scriptValidationEnabled: Boolean = config.getBoolean("app.script-validations.enabled")
  protected def isScriptValidationEnabled: Boolean = scriptValidationEnabled
  protected def scriptValidations(ctx: AppActionContext)(implicit locale: Locale): Unit = {
    if (isScriptValidationEnabled) {
      val (cn, fn) = OpParser.classNameFunctionName(config.getString("app.script-validations.init"))
      val module = invokeFunction(cn, fn,
        Seq(
          (classOf[DbAccess], () => this),
          (classOf[AppQuerease], () => qe),
          (classOf[ExecutionContext], () => ctx.ec),
        ))(ctx.ec).asInstanceOf[ScriptValidation]
      module.validate(ctx.viewName, ctx.actionName, ctx.values ++ ctx.env)
    }
  }
}

object WabaseAppConfig extends AppBase.AppConfig {
  val DefaultCp: PoolName = DEFAULT_CP
  val SerializationBufferSize: Int = appConfig.getBytes("serialization-buffer-size").toInt
  val SerializationBufferMaxFileSize: Long = MarshallingConfig.dbDataFileMaxSize
  val SerializationBufferMaxFileSizes: Map[String, Long] = MarshallingConfig.customDataFileMaxSizes
  def viewSerializationBufferMaxFileSize(viewName: String): Long =
    SerializationBufferMaxFileSizes.getOrElse(viewName, SerializationBufferMaxFileSize)
  val CurrentUserParameterName = appConfig.getString("current-user-key-name")
  val UserCredentialsParameterName = appConfig.getString("user-credentials-key-name")
}
