package org.wabase

import com.typesafe.config.ConfigFactory
import org.apache.pekko.http.scaladsl.model.{HttpMethod, Uri}
import org.mojoz.metadata.{FieldDef, TableMetadata, Type, ViewDef}
import org.mojoz.metadata.in._
import org.mojoz.metadata.io.MdConventions
import org.mojoz.metadata.out.DdlGenerator.SimpleConstraintNamingRules
import org.mojoz.querease.FilterType._
import org.mojoz.querease.{FilterType, QuereaseMetadata, TresqlJoinsParser, TresqlMetadata, ViewNotFoundException}
import org.tresql.{Cache, CacheBase, QueryParser, SimpleCache, SimpleCacheBase, ast}
import org.tresql.ast.{Exp, Variable}
import org.tresql.parsing.QueryParsers
import org.wabase.AppMetadata.JobCall
import org.wabase.AppMetadata.Action.TresqlExtraction.{OpTresqlTraverser, State, StepTresqlTraverser, opTresqlTraverser, stepTresqlTraverser}
import org.wabase.AppMetadata.Action.{OpTraverser, StepTraverser, Validations, ViewCall, traverseAction}

import java.io.InputStream
import java.util.concurrent.TimeUnit
import scala.collection.immutable.{Map, Seq, Set}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

trait AppMetadata extends QuereaseMetadata { this: AppQuerease =>

  import AppMetadata._

  val knownApiMethods: Set[String] = AppMetadata.Action() - AppMetadata.Action.Job
  private val fullKeyOps = Set("get",/*insert*/ "update", "update+", "upsert", "save", "delete", "put")
  override lazy val yamlMetadata = YamlMd.fromPaths(Seq("jobs", "routes", "tables", "views"))
  override lazy val uninheritableExtras: Seq[String] = Seq("api")
  lazy val knownViewExtras = KnownViewExtras()
  lazy val knownPrefixes = Set(KnownViewExtras.Auth)
  val knownAuthOps = KnownAuthOps()
  lazy val knownFieldExtras = KnownFieldExtras()

  lazy val defaultCpName = TresqlResourcesConf.DefaultCpName

  /** Set to false to compile actions when invocation target classes are not (yet) available, defaults to true for runtime to fail fast */
  lazy val checkInvocations = true

  /** Get macro class from 'main' tresql resources config */
  override lazy val macrosClass: Class[_] =
    TresqlResourcesConf.confs.get(null)
      .flatMap(c => Option(c.macros))
      .map(_.getClass)
      .getOrElse(classOf[Macros])
  override lazy val joinsParser: JoinsParser =
    new TresqlJoinsParser(
      // to avoid stack overflow cannot use directly field tresqlMetadata,
      // since it is initialized with viewDefs (for cursor table metadata)
      TresqlMetadata(tableMetadata.tableDefs, typeDefs, macrosClass, resourceClassLoader, aliasToDb),
      createJoinsParserCache(_)
    )
  override protected lazy val macrosInstance = Option(macrosClass).map(getObjectOrNewInstance(_, "metadata macros")).orNull
  override lazy val metadataConventions: AppMdConventions = new DefaultAppMdConventions(resourceLoader)()
  override lazy val viewDefLoader: YamlViewDefLoader =
    new YamlViewDefLoader(tableMetadata, yamlMetadata, joinsParser, metadataConventions, uninheritableExtras, typeDefs) {
      override protected def isViewDef(m: Map[String, _]) = {
        !m.contains("columns") && !m.contains("on") && !m.contains("type")
      }
    }
  override lazy val nameToViewDef: Map[String, ViewDef] =
    toAppViewDefs(viewDefLoader.nameToViewDef)

  private lazy val publicViewsLocationPattern = config.getString("app.public-api.views-location-pattern").r
  private lazy val publicViewNames: Set[String] = {
    yamlMetadata
      .filter(md => publicViewsLocationPattern.pattern.matcher(md.filename).matches())
      .flatMap(_.parsed.flatMap(_.get("name").toSeq).filter(_ != null).map(_.toString)).toSet
      .filter(viewDefLoader.nameToViewDef.contains)
  }
  def isPublicView(viewName: String) = publicViewNames.contains(viewName)

  /* view paths are calculated outside of toAppViewDef method because of config parameters usage which
  * are not available in sbt-mojoz plugin. */
  private lazy val viewPaths: Map[String, Seq[Uri.Path]] = {
    nameToViewDef.map { case (n, vd) =>
      val pathPrefix = config.getString("app.views-api.uri-prefix")
      val publicPrefix = config.getString("app.public-api.views-uri-prefix")
      val paths = vd.paths match {
        case Nil =>
          val prefix = Uri.Path(if (isPublicView(vd.name)) publicPrefix else pathPrefix)
          def maybePaths(apis: Seq[String]) = apis.collect {
            case api if vd.apiMethodToRoles.contains(api) => prefix ?/ s"${vd.name}:$api"
          }
          Seq(prefix ?/ vd.name) ++ maybePaths(Seq(Action.Count, Action.Create))
        case paths  => paths.map(p => if (p.startsWith("/")) Uri.Path(p) else Uri.Path(pathPrefix) ?/ p)
      }
      (n, paths)
    }
  }

  def allowedPaths(viewName: String): Seq[Uri.Path] = viewPaths.getOrElse(viewName,
    throw ViewNotFoundException(s"View definition for $viewName not found"))

  lazy val routeDefLoader = {
    val actionParser: String => String => Map[String, Any] => Action =
      objectName => dataKey => dataMap => {
        val opParser = new OpParser(objectName, tableMetadata, resourceClassLoader)
        parseOrCacheAction(ViewDefExtrasUtils.getSeq(dataKey, dataMap), opParser)
      }
    new YamlRouteDefLoader(yamlMetadata, actionParser)
  }
  lazy val routeDefs: Seq[RouteDef] = routeDefLoader.routeDefs

  protected def isActionCacheUpdatable: Boolean = false
  /** This cache is updated on view metadata loading if isActionCacheUpdatable */
  protected lazy val actionCache: CacheBase[Action] =
    ActionCache.createCache(ActionCache.loadSerializedCache(resourceLoader), parserCacheSize)

  protected lazy val joinsParserCache: Map[String, Map[String, Exp]] =
    loadJoinsParserCache(resourceLoader)
  override lazy val viewNameToQueryVariablesCache: Map[String, Seq[ast.Variable]] =
    loadViewNameToQueryVariablesCache(resourceClassLoader)

  def toAppViewDefs(mojozViewDefs: Map[String, ViewDef]) = {
    val viewDefs = transformAppViewDefs {
      val inlineViewDefNames =
        mojozViewDefs.values.flatMap { viewDef =>
          viewDef.fields.filter { field =>
            field.type_.isComplexType &&
              field.type_.name == viewDef.name + "_" + field.name // XXX
          }.map(_.type_.name)
        }.toSet
      mojozViewDefs.transform { (_, v) => toAppViewDef(v, isInline = inlineViewDefNames.contains(v.name)) }
    }
    checkInvocations(viewDefs)
    viewDefs
  }

  protected def transformAppViewDefs(viewDefs: Map[String, ViewDef]): Map[String, ViewDef] =
    Option(viewDefs)
      .map(resolveViewDbAccessKeys)
      .orNull

  override def viewNameFromMf[T <: AnyRef](implicit mf: Manifest[T]): String =
    classToViewNameMap.getOrElse(mf.runtimeClass, mf.runtimeClass.getSimpleName)

  def dtoMappingClassName = "dto.DtoMapping"
  def defaultApiRoleName  = "ADMIN"

  lazy val viewNameToClassMap: Map[String, Class[_ <: Dto]] = {
    val objectClass = Class.forName(dtoMappingClassName + "$")
    val module = objectClass.getField("MODULE$").get(objectClass)
    module.getClass.getMethod("viewNameToClass").invoke(module)
      .asInstanceOf[Map[String, Class[_ <: Dto]]]
  }

  lazy val classToViewNameMap: Map[Class[_], String] = viewNameToClassMap.map(_.swap)

  lazy val viewNameToApiKeyFields: Map[String, Seq[FieldDef]] =
    viewNameToKeyFields.map { case (name, fields) => (name, fields.filterNot(_.api.excluded)) }
  lazy val viewNameToApiKeyColNames: Map[String, Seq[String]] =
    viewNameToApiKeyFields.map { case (name, fields) => (name, fields.map(_.name)) }
  lazy val viewNameToApiKeyFieldNames: Map[String, Seq[String]] =
    viewNameToApiKeyFields.map { case (name, fields) => (name, fields.map(_.fieldName)) }

  def splitToLabelAndComments(s: String): (String, String) = {
    def clear(s: String) = Option(s).filter(_ != "").orNull
    def unescape(s: String) = s.replace("--", "-")
    Option(s).map(_.trim.split("(^|\\s+)-(\\s+|$)", 2).toList match {
      case List(label) => (clear(unescape(label)), null)
      case List(label, comments) => (clear(unescape(label)), clear(comments))
      case x => throw new IllegalStateException("impossible: " + x)
    }) getOrElse (null, null)
  }

  def collectViews[A](f: PartialFunction[ViewDef, A]): Iterable[A] = nameToViewDef.values.collect(f)

  protected val handledViewExtras = KnownViewExtras() - KnownViewExtras.QuereaseViewExtrasKey
  protected val handledFieldExtras = KnownFieldExtras() - KnownFieldExtras.QuereaseFieldExtrasKey
  lazy val knownInlineViewExtras = knownViewExtras ++ knownFieldExtras

  protected def isSortableField(viewDef: ViewDef, f: FieldDef) = {
    FieldDefExtrasUtils.getBooleanExtraOpt(viewDef, f, KnownFieldExtras.Sortable) getOrElse {
      if (f.orderBy != null && viewDef.table != null && (
        !f.type_.isComplexType || viewDefOption(f.type_.name).exists { childView =>
          childView.table == null && (childView.joins == null || childView.joins == Nil)
        }
      )) true
      else
      if (f.isExpression || f.isCollection || viewDef.table == null) false
      else f.table == viewDef.table && {
        val td = tableMetadata.tableDef(f.table, viewDef.db)
        td .pk.exists(_.cols(0) == f.name) ||
        td .uk.exists(_.cols(0) == f.name) ||
        td.idx.exists(_.cols(0) == f.name)
      }
      // TODO follow inner joins?
    }
  }

  protected def toAppFieldDef(viewDef: ViewDef, f: FieldDef): FieldDef = {
      import KnownFieldExtras._
      import ViewDefExtrasUtils.getStringSeq
      import FieldDefExtrasUtils._

      val (label, comments) =
        Option(f.comments).map(splitToLabelAndComments).map{
          case (null, c) => (fieldLabelFromName(f), c)
          case lc => lc
        } getOrElse (fieldLabelFromName(f), null)
      val fieldName = f.fieldName
      def isPk = viewDef.table != null &&
        tableMetadata.tableDefOption(viewDef).flatMap(_.pk).map(_.cols).contains(Seq(f.name))

      val fieldApiKnownOps = Set(Readwrite, Readonly, NoInsert, NoUpdate, Excluded)
      val fieldApi = getStringSeq(FieldApi, f.extras) match {
        case api =>
          val ops = api.flatMap(_.trim.split(",").toList).map(_.trim).filter(_ != "").toSet
          val opt = fieldOptionsSelf(f)
          def op(opkey: String) = (ops contains opkey) || api.isEmpty && (opkey match {
            case Excluded => false
            case Readonly => opt != null &&  (opt contains "!")
            case NoInsert => opt != null && !(opt contains "+")
            case NoUpdate => opt != null && !(opt contains "=") || isPk
          })
          val unknownOps = ops -- fieldApiKnownOps
          if (unknownOps.nonEmpty)
            sys.error(
              s"Unknown $FieldApi value(s), viewDef field ${viewDef.name}.${fieldName}, value(s): ${unknownOps.mkString(", ")}")
          val ro = op(Readonly)
          FieldApiOps(
            insertable = !ro && !op(Excluded) && !op(NoInsert),
            updatable  = !ro && !op(Excluded) && !op(NoUpdate),
            excluded   = op(Excluded),
          )
      }

      val required = getBooleanExtraOpt(viewDef, f, Required)
                        .getOrElse(viewDef.table == f.table && !f.nullable && !isPk &&
                                   !fieldApi.readonly && !fieldApi.excluded)
      val sortable = isSortableField(viewDef, f)
      val hiddenOpt = getBooleanExtraOpt(viewDef, f, Hidden)
      val visibleOpt = getBooleanExtraOpt(viewDef, f, Visible)
      if (hiddenOpt.isDefined && visibleOpt.isDefined && hiddenOpt == visibleOpt)
        sys.error(s"Conflicting values of visible and hidden, viewDef field: ${viewDef.name}.${fieldName}")
      val visible = hiddenOpt.map(! _).getOrElse(visibleOpt getOrElse true)

      val knownExtras =
        if (f.type_.isComplexType) knownFieldExtras ++ knownViewExtras ++
          Option(f.extras).map(_.keySet).getOrElse(Set.empty).filter(k =>
            knownPrefixes.exists(k startsWith _ + " ")) // auth can be used as prefix, too
        else knownFieldExtras
      val handledExtras =
        if (f.type_.isComplexType) handledFieldExtras ++ handledViewExtras ++
          Option(f.extras).map(_.keySet).getOrElse(Set.empty).filter(k =>
            knownPrefixes.exists(k startsWith _ + " ")) // auth can be used as prefix, too
        else handledFieldExtras
      val extras =
        Option(f.extras)
          .map(_ -- handledExtras)
          .map { x =>
            val normalizedSwagger =
              f.extras.get(KnownViewExtras.Swagger).map {
                case m: java.util.Map[String @unchecked, _] => Map(KnownViewExtras.Swagger -> MapUtils.javaMapToMap(m))
                case x => Map(KnownViewExtras.Swagger -> x)
              }.getOrElse(Map.empty)
            x ++ normalizedSwagger
          }
          .orNull
      val unknownKeys =
        Option(extras)
          .map(_ -- knownExtras)
          .map(_.keySet.filterNot(_.toLowerCase startsWith "todo"))
          .filterNot(_.isEmpty).orNull
      if (unknownKeys != null)
        sys.error(
          s"Unknown or misplaced properties for viewDef field ${viewDef.name}.${fieldName}: ${unknownKeys.mkString(", ")}")

      import f._
      FieldDef(table, tableAlias, name, alias, options, isOverride, isCollection,
        isExpression, expression, f.saveTo, resolver, nullable,
        type_, enum_, joinToParent, orderBy,
        comments, extras)
      .updateWabaseExtras(_ => AppFieldDef(fieldApi, label, required, sortable, visible))
  }

  protected def toAppViewDef(vd: ViewDef, isInline: Boolean): ViewDef = {
    import KnownViewExtras._
    import ViewDefExtrasUtils._
    val viewDef = toQuereaseViewDef(vd)
    val appFields = viewDef.fields.map(toAppFieldDef(viewDef, _))

    val keyFields = Option(viewDef.keyFields).map(_.map(toAppFieldDef(viewDef, _))).orNull

    import viewDef._
    val auth = toAuth(viewDef, Auth, knownAuthOps)

    def badApiStructure = s"Unexpected API methods and roles structure for view ${viewDef.name}"
    val api =
      (getSeq(Api, viewDef.extras).map {
        case m: java.util.Map[_, _] =>
          if (m.size == 1) {
            val entry = m.entrySet.asScala.toList(0)
            s"${entry.getValue.toString} ${entry.getKey.toString}"
          } else sys.error(badApiStructure)
        case x => x.toString
      }).flatMap { s =>
       val parts = s.trim.split("[\\s,]+").toList.filter(_ != "")
       val lastOpt = parts.lastOption
       if (lastOpt.nonEmpty && !lastOpt.exists(knownApiMethods.contains))
          sys.error(badApiStructure)
       parts
      }

    val apiToRoles = api.foldLeft((Map[String, Set[String]](), Set(defaultApiRoleName), true)) {
      case ((apiToRoles, roles, canReset), x) =>
        if (knownApiMethods contains x)
          if (apiToRoles contains x)
                sys.error(s"Duplicate API method definition: ${viewDef.name}.$x")
          else (apiToRoles + (x -> roles), roles, true)
        else if (canReset)
          (apiToRoles, Set(x), false)
        else
          (apiToRoles, roles + x, false)
    }._1

    val limit = getIntExtra(Limit, viewDef) getOrElse 100
    val paths = getStringSeq(Paths, viewDef.extras)
    val explicitDb = getBooleanExtra(ExplicitDb, viewDef)
    val (decoder, maxContentSize) = getStringExtra(Decoder, viewDef)
      .map(parseDecoder(viewDef.name, _)).getOrElse((DefaultDecoder, null))
    val timeout = parseTimeout(viewDef.name, getStringExtra(Timeout, viewDef).orNull)
    val sqlTimeout = parseTimeout(viewDef.name, getStringExtra(SqlTimeout, viewDef).orNull)
    val opParser = new OpParser(viewDef.name, tableMetadata, resourceClassLoader)
    val actions = Action().foldLeft(Map[String, Action]()) { (res, actionName) =>
      val a = parseOrCacheAction(getSeq(actionName, viewDef.extras), opParser)
      if (a.steps.nonEmpty) res + (actionName -> a) else res
    }
    val maxKeySize            = viewDef.keyFieldNames.size
    val hasFullKeyOps         = fullKeyOps.exists(apiToRoles.contains)
    val (minKeySizeForCollection, maxKeySizeForCollection) =
      if  (!hasFullKeyOps || viewDef.minSearchKeyFieldCount != maxKeySize)
           (viewDef.minSearchKeyFieldCount,
            if (hasFullKeyOps) math.max(0, maxKeySize - 1) else maxKeySize)
      else (0, 0)

    val extras =
      Option(viewDef.extras)
        .map(_ -- handledViewExtras)
        .map(_.filterNot(e => knownPrefixes.exists(e._1 startsWith _ + " "))) // auth can be used as prefix, too
        .map { x =>
          val normalizedSwagger =
            viewDef.extras.get(KnownFieldExtras.Swagger).map {
              case m: java.util.Map[String @unchecked, _] => Map(KnownFieldExtras.Swagger -> MapUtils.javaMapToMap(m))
              case x => Map(KnownFieldExtras.Swagger -> x)
            }.getOrElse(Map.empty)
          x ++ normalizedSwagger
        }
        .orNull
    val unknownKeys =
      Option(extras)
        .map(_ -- (if (isInline) knownInlineViewExtras else knownViewExtras))
        .map(_.keySet.filterNot(_.toLowerCase startsWith "todo"))
        .filterNot(_.isEmpty).orNull
    if (unknownKeys != null)
      sys.error(
        s"Unknown properties for viewDef ${viewDef.name}: ${unknownKeys.mkString(", ")}")

    ViewDef(name, db, table, tableAlias, column, distinct, joins, filter,
      viewDef.groupBy, viewDef.having, orderBy, extends_,
      comments, appFields, viewDef.saveTo, extras)
      .updateExtras(_.copy(keyFields = keyFields))
      .updateWabaseExtras(_ =>
        AppViewDef(limit, paths, explicitDb, decoder, maxContentSize, timeout, sqlTimeout,
          auth, apiToRoles, actions, Map.empty, minKeySizeForCollection, maxKeySizeForCollection))
  }

  override protected def keyFields(view: ViewDef): Seq[FieldDef] =
    Option(view.keyFields).getOrElse(super.keyFields(view).map { f =>
      if (f.extras != null && f.extras.contains(WabaseFieldExtrasKey)) f
      else toAppFieldDef(view, f)
    })

  override protected def serializedCaches: Map[String, Array[Byte]] = {
    import io.bullet.borer._
    val joinData: Map[String, Map[String, Exp]] =
      joinsParser.asInstanceOf[TresqlJoinsParser].dbToCompilerAndCache.map {
        case (db, (_, c)) => Option(db).getOrElse("null") -> c.map(_.toMap).getOrElse(Map[String, Exp]())
      }

    import CacheIo.expCodec
    val serializedJoins = Map(JoinsCompilerCacheName -> Cbor.encode(joinData).toByteArray)

    val serializedQeParserCache = parser.cache.map { cache =>
      Map(AppQuereaseParserCacheName -> Cbor.encode(cache.toMap).toByteArray)
    }.getOrElse(Map.empty)

    super.serializedCaches ++
      serializedQeParserCache ++
      ActionCache.serializeCache(actionCache) ++
      serializedJoins
  }

  protected def createJoinsParserCache(db: String): Option[Cache] =
    joinsParserCacheFactory(joinsParserCache, parserCacheSize)(db)

  private def resolveDbAccessKeys(
    action: String, name: String,
    viewDefs: Map[String, ViewDef],
    fun: (=>StepTresqlTraverser[Seq[DbAccessKey]]) => State[Seq[DbAccessKey]] => State[Seq[DbAccessKey]]
  ): State[Seq[DbAccessKey]] = {
    lazy val opTresqlTrav: OpTresqlTraverser[Seq[DbAccessKey]] =
      opTresqlTraverser(opTresqlTrav, stepTresqlTrav)(state => {
        case Action.Db(action, _, dbs) =>
          traverseAction(action)(stepTresqlTrav)(state.copy(value = state.value ++ dbs))
      })

    lazy val stepTresqlTrav: StepTresqlTraverser[Seq[DbAccessKey]] =
      stepTresqlTraverser(opTresqlTrav)(state => {
        case Validations(_, _, dbkey) => state.copy(value = state.value ++ dbkey.toList)
      })

    val state = State[Seq[DbAccessKey]](action, name, viewDefs,
      tresqlExtractor = dbKeys => tresql => dbKeys ++ tresql.dbs
        .filter(_.db != null).map(d => DbAccessKey(d.db)),
      viewExtractor = dbkeys => vd =>
        dbkeys ++ (if (vd.db != null) Seq(DbAccessKey(vd.db)) else Nil),
      processed = Set(), value = Nil
    )
    fun(stepTresqlTrav)(state)
  }

  protected def resolveViewDbAccessKeys(
    viewDefs: Map[String, ViewDef],
  ): Map[String, ViewDef] = {
    viewDefs.transform { case (viewName, viewDef) =>
      import Action._
      import TresqlExtraction._
      val actionToDbAccessKeys = Action().map { case action =>
        val action_ = viewDef.actions.get(action).map(_ => action).getOrElse(action match {
          case Action.Insert | Action.Update | Action.Upsert if (viewDef.actions.get(Action.Save).nonEmpty) =>
            Action.Save
          case _ =>
            action
        })
        val st = resolveDbAccessKeys(action_, viewName, viewDefs, processView[Seq[DbAccessKey]])
        val dbkeys = st.value.distinct
        (action, dbkeys)
      }.toMap
      viewDef.updateWabaseExtras(_.copy(actionToDbAccessKeys = actionToDbAccessKeys))
    }
  }

  protected def checkInvocations(viewDefs: Map[String, ViewDef]): Unit = {
    if (checkInvocations) {
      lazy val opTrav: OpTraverser[Unit] =
        Action.opTraverser(opTrav, stepTrav)(_ => {
          case Action.Invocation(cn, fn, args, _) =>
            getObjAndFunction(cn, fn)
            args foreach opTrav(())
        })
      lazy val stepTrav: StepTraverser[Unit] =
        Action.stepTraverser(opTrav)(_ => PartialFunction.empty)
      viewDefs.foreach { case (viewName, viewDef) =>
        viewDef.actions.foreach { case (actionName, action) =>
          try Action.traverseAction(action)(stepTrav)(()) catch {
            case NonFatal(e) => throw new RuntimeException(
              s"Unable to resolve invocation in $viewName.$actionName: '${e.getMessage}'", e
            )
          }
        }
      }
    }
  }

  protected def parseOrCacheAction(stepData: Seq[Any], opParser: OpParser): Action = {
    val objectHash = AppMetadata.sha256(Map(opParser.viewName -> stepData))
    actionCache.get(objectHash).getOrElse {
      val act = parseAction(objectHash, stepData, opParser)
      if (isActionCacheUpdatable) actionCache.put(objectHash, act)
      act
    }
  }

  protected def parseAction(objectName: String, stepData: Seq[Any], opParser: OpParser): Action = {
    // matches - 'validations validation_name [db:cp]'
    val validationRegex = new Regex(s"(?U)${Action.ValidationsKey}(?:\\s+(\\w+))?(?:\\s+\\[(?:\\s*(\\w+)?\\s*(?::\\s*(\\w+)\\s*)?)\\])?")
    import ViewDefExtrasUtils._
    val steps = stepData.map { step =>
      def parseStep(anyStep: Any): (Action.Step, String) = {
        anyStep match {
          case s: String => (opParser.parseStep(s), s)
          case n: java.lang.Number => (opParser.parseStep(n.toString), n.toString)
          case b: java.lang.Boolean => (opParser.parseStep(b.toString), b.toString)
          case null => (opParser.parseStep("null"), "null")
          case jm: java.util.Map[String, Any]@unchecked if jm.size() == 1 =>
            val m = jm.asScala.toMap
            val (operationString, value) = m.head
            if (validationRegex.pattern.matcher(operationString).matches()) {
              val validationRegex(vn, db, cp) = operationString: @unchecked
              val validations = getSeq(operationString, m).map(_.toString)
              (Action.Validations(
                Option(vn),
                validations,
                if (db == null) None else Option(DbAccessKey(db))
              ), "validation" + Option(vn).map(n => s" [$n]").mkString)
            } else {
              value match {
                case jm: java.util.Map[String@unchecked, _] =>
                  // may be 'if', 'foreach', 'db ...' step
                  parseStep(jm)
                case al: java.util.ArrayList[_] =>
                  // 'if', 'foreach', 'db ...' step
                  def pa = parseAction(objectName, al.asScala.toList, opParser)
                  def addBlock(op: Action.Op) = op.asInstanceOf[Action.BlockOp] match {
                    case bl: Action.If      => if (bl.action == null) bl.copy(action = pa) else bl.copy(elseAct = pa)
                    case bl: Action.Foreach => bl.copy(action = pa)
                    case bl: Action.Db      => bl.copy(action = pa)
                    case bl: Action.Else    => bl.copy(action = pa)
                    case bl: Action.Block   => bl.copy(action = pa)
                    case null               => Action.Block(pa)
                  }
                  val step = opParser.parseStep(operationString, isBlock = true) match {
                    case st: Action.Evaluation  => st.copy(op     = addBlock(st.op))
                    case st: Action.SetEnv      => st.copy(value  = addBlock(st.value))
                    case st: Action.Return      => st.copy(value  = addBlock(st.value))
                    case st                     => sys.error(s"Unexpected operation: $operationString")
                  }
                  (step, operationString)
                case _ => // the same as evaluation step like <variable_name> = <expression>
                  (opParser.parseStep(s"$operationString = $value"), s"$operationString = $value")
              }
            }
          case x =>
            sys.error(s"'$objectName' parsing error. Unable to parse value: '$x'")
        }
      }
      parseStep(step)
    }.toList
    //coalesce else op into if
    val coalesced_if_else_steps = if (steps.isEmpty) Nil else
      (steps.tail.foldLeft(steps.head -> List[(Action.Step, String)]()) { case ((prev_st, r), (s, src)) =>
        (prev_st, s) match {
          case ((p, psrc), Action.Evaluation(_, _, elseOp: Action.Else)) => p match {
            case ifEv@Action.Evaluation(_, _, ifOp: Action.If) =>
              (null, (ifEv.copy(op = ifOp.copy(elseAct = elseOp.action)), psrc) :: r)
            case ifSetEnv@Action.SetEnv(_, _, ifOp: Action.If, _) =>
              (null, (ifSetEnv.copy(value = ifOp.copy(elseAct = elseOp.action)), psrc) :: r)
            case ifReturn@Action.Return(_, _, ifOp: Action.If) =>
              (null, (ifReturn.copy(value = ifOp.copy(elseAct = elseOp.action)), psrc) :: r)
            case _ => sys.error(s"else statement must follow if statement, instead found '$p'")
          }
          case _ => ((s, src), if (prev_st != null) prev_st :: r else r)
        }
      } match {
        case (null, r) => r
        case (x, r) => x :: r
      }).reverse
    Action(coalesced_if_else_steps)
  }

  protected def parseDecoder(viewName: String, decStr: String): (RequestDecoder, jLong) = {
    val decPattern = new Regex(s"(none|default|${OpParser.InvocationRegex})(.*)")
    if (decPattern.pattern.matcher(decStr).matches()) {
      val decPattern(dec, _, size) = decStr: @unchecked
      (dec match {
        case "default" => DefaultDecoder
        case "none" => NoneDecoder
        case x =>
          val idx = x.lastIndexOf('.')
          CustomDecoder(x.substring(0, idx), x.substring(idx + 1))
      }, if (size.trim.isEmpty) null else {
        val propName = s"wabase.$viewName.upload.size.limit"
        val cf = ConfigFactory.parseString(s"$propName = ${size.trim}").withFallback(config).resolve()
        cf.getBytes(propName)
      })
    } else throw new IllegalArgumentException(s"Decoder string does not match pattern: " +
    s"<none|default|<custom function>> [<max content size>]")
  }

  protected def parseTimeout(viewName: String, timeoutStr: String): FiniteDuration = {
    if (timeoutStr == null) null
    else {
      val propName = s"wabase.$viewName.timeout"
      val d = ConfigFactory.parseString(s"$propName = $timeoutStr").withFallback(config).resolve()
        .getDuration(propName)
      FiniteDuration(d.toSeconds, TimeUnit.SECONDS)
    }
  }

  val AppQuereaseParserCacheName  = "app-querease-parser-cache.cbor"
  override protected def createParserCache: Option[Cache] = {
    def loadAppQuereaseParserCache(getResourceAsStream: String => InputStream): Map[String, Exp] = {
      val res = getResourceAsStream(s"/$AppQuereaseParserCacheName")
      if (res == null) {
        logger.debug(s"No app querease parser cache resource - '/$AppQuereaseParserCacheName' found")
        Map()
      } else {
        import io.bullet.borer._
        import CacheIo.expCodec
        val cache = try Cbor.decode(res).to[Map[String, Exp]].value catch {
          case NonFatal(e) => throw new RuntimeException(
            s"Error reading parsed view cache from /$AppQuereaseParserCacheName. " +
              s"Please delete file explicitly or by calling 'sbt clean'", e)
        }
        logger.debug(s"App querease parser cache loaded for ${cache.size} expressions")
        cache
      }
    }
    val cache = new SimpleCache(parserCacheSize)
    cache.load(loadAppQuereaseParserCache(resourceLoader))
    Some(cache)
  }

  private val vname = "[_\\p{IsLatin}][_\\p{IsLatin}0-9]*"
  private val ContainsOpFilterDef = s"^.*%~+%\\s*:($vname)\\??$$".r
  private val EndsWithOpFilterDef = s"^.*%~+\\s*:($vname)\\??$$".r
  private val StartsWithOpFilterDef = s"^.*~+%\\s*:($vname)\\??$$".r
  def filterFieldLabel(name: String, colLabel: String, filterType: FilterType): FilterLabel = {
    import org.mojoz.querease.FilterType._
    filterType match {
      case ComparisonFilter(col, op, name, opt) =>
        op match {
          // TODO more operators?
          case "%~~~%" | "%~~%" | "%~%" => FilterLabel(colLabel, "contains")
          case "%~~~" | "%~~" | "%~" => FilterLabel(colLabel, "ends with")
          case "~~~%" | "~~%" | "~%" => FilterLabel(colLabel, "begins with")
          case _ => FilterLabel(colLabel, null)
        }
      case IntervalFilter(nameFrom, optFrom, opFrom, col, opTo, nameTo, optTo) =>
        if (name == nameFrom) FilterLabel(colLabel.replace(" from", ""), "from")
        else if (name == nameTo) FilterLabel(colLabel.replace(" to", ""), "to")
        else FilterLabel(colLabel, null)
      case OtherFilter(fExpr) => fExpr match {
        case ContainsOpFilterDef(vName) if vName == name => FilterLabel(colLabel, "contains")
        case EndsWithOpFilterDef(vName) if vName == name => FilterLabel(colLabel, "ends with")
        case StartsWithOpFilterDef(vName) if vName == name => FilterLabel(colLabel, "begins with")
        case _ => FilterLabel(colLabel, null)
      }
      case _ => FilterLabel(colLabel, null)
    }
  }
  def filterToParameterNamesAndCols(filter: FilterType): Seq[(String, String)] = filter match {
    case BooleanFilter(b) =>
      Nil
    case IdentFilter(col, name, opt) =>
      Seq(name -> col)
    case ComparisonFilter(col, op, name, opt) =>
      Seq(name -> col)
    case IntervalFilter(nameFrom, optFrom, opFrom, col, opTo, nameTo, optTo) =>
      Seq(nameFrom -> col, nameTo -> col)
    case RefFilter(col, name, opt, refViewName, refFieldName, refCol) =>
      Seq(name -> col)
    case OtherFilter(_) =>
      Nil
    case _ =>
      Nil
  }
  def filterToParameterNames(filter: FilterType): Seq[String] = filter match {
    case BooleanFilter(b) =>
      Nil
    case IdentFilter(col, name, opt) =>
      Seq(name)
    case ComparisonFilter(col, op, name, opt) =>
      Seq(name)
    case IntervalFilter(nameFrom, optFrom, opFrom, col, opTo, nameTo, optTo) =>
      Seq(nameFrom, nameTo)
    case RefFilter(col, name, opt, refViewName, refFieldName, refCol) =>
      Seq(name)
    case OtherFilter(fExpr) =>
      parser.extractVariables(fExpr)
        .map(_.variable)
    case _ =>
      Nil
  }
  private val filterParametersParserCache = new SimpleCache(parserCacheSize)
  def filterParameters(view: ViewDef): Seq[FilterParameter] = {
    def fieldNameToLabel(n: String) =
      n.replace("_", " ").capitalize
    val v = view
    val hasTableOrJoins = v.table != null || v.joins != null && v.joins.nonEmpty
    if (v.apiMethodToRoles != null && v.apiMethodToRoles.nonEmpty &&
          (hasTableOrJoins || Option(v.filter).getOrElse(Nil).nonEmpty)) {
      val filters =
        Option(v.filter).getOrElse(Nil) flatMap { f =>
          analyzeFilter(f, v, v.tableAlias)
        }

      // TODO duplicate code, reuse querease code!
      def simpleName(name: String) = if (name == null) null else name.lastIndexOf('.') match {
        case -1 => name
        case  i => name.substring(i + 1)
      }
      def tailists[B](l: List[B]): List[List[B]] =
        if (l.isEmpty) Nil else l :: tailists(l.tail)
      val (needsBaseTable, parsedJoins) =
        Option(v.joins)
          .map(joins =>
            Try(joinsParser(v.db, null, joins)).toOption
              .map(joins => (false, joins))
              .getOrElse((true, joinsParser(v.db, tableAndAlias(v), joins))))
          .getOrElse((false, Nil))
      val joinAliasToTables: Map[String, Set[String]] =
        parsedJoins.map(j => Option(j.alias).getOrElse(j.table) -> j.table).toSet
          .filter(_._1 != null)
          .flatMap { case (n, t) => tailists(n.split("\\.").toList).map(_.mkString(".") -> t) }
          .groupBy(_._1)
          .map { kkv => kkv._1 -> kkv._2.map(_._2).toSet }
      val baseQualifier = baseFieldsQualifier(view)
      val aliasToTable = collection.mutable.Map[String, String]()
      if (baseQualifier != null) {
        if (view.table != null)
          // FIXME exclude clashing simple names from different qualified names!
          aliasToTable += (baseQualifier -> view.table)
          aliasToTable += (simpleName(baseQualifier) -> view.table)
      }
      aliasToTable ++= joinAliasToTables.filter(_._2.size == 1).map { case (n, t) => n -> t.head }
      // -----------------------------------------

      val parameterNameToCol =
        filters.flatMap(filterToParameterNamesAndCols).toMap
      val parameterNameToFilterType =
        filters.flatMap(filter => filterToParameterNames(filter).map(_ -> filter)).toMap
      val allVariables =
        viewNameToQueryVariablesCache.getOrElse(v.name, {
          val q =
            if (hasTableOrJoins) queryStringAndParams(v, Map.empty)._1
            else                 s"null${where(v, null)}"
          new QueryParser(macroResources, filterParametersParserCache).extractVariables(q)
        })
      // TODO? fromAndPathToAlias(v): (String, Map[List[String], String])
      val groupedVariables = allVariables.groupBy(_.variable)
      allVariables
        .map(_.variable)
        .distinct
        .map(name => groupedVariables(name).maxBy(_.opt))
        .map { v =>
          val colQName = parameterNameToCol.getOrElse(v.variable, "")
          val filterType = parameterNameToFilterType.get(v.variable).orNull
          val refViewName = Option(filterType).map {
            case RefFilter(col, name, opt, refViewName, refFieldName, refCol) => refViewName
            case _ => null
          }.orNull
          val tableAlias =
            if (colQName.indexOf(".") > 0)
              colQName.substring(0, colQName.indexOf("."))
            else Option(view.tableAlias).getOrElse(view.table)
          val colName =
            if (colQName.indexOf(".") > 0)
              colQName.substring(colQName.indexOf(".") + 1)
            else colQName
          val col = aliasToTable
            .get(tableAlias)
            .map(tableName => tableMetadata.tableDefOption(tableName, view.db).map(_.cols) getOrElse Nil)
            .flatMap(_.find(_.name == colName))
            .orNull
          val name = v.variable
          val conventionsType =
            metadataConventions.typeFromExternal(name, None)
          val table = aliasToTable.getOrElse(tableAlias, null)
          val label = Option(col)
            .map(_.comments)
            .filter(_ != null)
            .filter(_ != "")
            .map(splitToLabelAndComments(_)._1)
            .filter(_ != null)
            .orElse(Option(fieldNameToLabel(name)))
            .map(filterFieldLabel(name, _, filterType))
            .orNull
          val nullable = v.opt
          val required = !v.opt
          val type_ = Option(col)
            .filter { col =>
              filterType.isInstanceOf[IdentFilter] ||
              filterType.isInstanceOf[ComparisonFilter] ||
              filterType.isInstanceOf[IntervalFilter]
            }
            .map(_.type_)
            .getOrElse(conventionsType)
          val enum_ = Option(col).map(_.enum_).orNull
          FilterParameter(name, table, label, nullable, required, type_, enum_, refViewName, filterType)
        }
    } else Nil
  }
}

class OpParser(val viewName: String, tmd: TableMetadata, cl: ClassLoader)
  extends QueryParsers { self =>
  import AppMetadata.Action._
  import AppMetadata.Action

  /** View action must be end with whitespace regexp so that no match is if space(s) is omitted between action and
    * view name since spaces are eliminated at the beginning of input before applying parser */
  val ActionRegex = new Regex(Action().map(a => if (a == Action.Job) JobCall else a)
    .mkString("(?U)(", "|", """)(?=\s+)"""))
  val ViewNameRegex = "(?U)\\w+".r
  val ConfPropRegex = """\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*(?:[.-]\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*+)*""".r
  val HttpClientFileStreamerNameRegex = """\w+(-\w+)*""".r
  val RedirectOpRegex = """redirect\s+""".r
  val RedirectToKeyRegex = """[_\p{IsLatin}][_\p{IsLatin}0-9]*$""".r

  def parseStep(step: String): Step = parseStep(step, isBlock = false)

  def parseStep(step: String, isBlock: Boolean): Step =
    phrase(this.step(isBlock))(new scala.util.parsing.input.CharSequenceReader(step)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }

  def step(isBlock: Boolean): Parser[Step] = { // returns Parser not MemParser because is dependant on parameter
    def op: Parser[Op] = if (isBlock) blockOp else operation
    def opWithOptVarTransforms: Parser[(List[VariableTransform], Op)] = {
      def value: Parser[ast.Exp] = (variable | const | "this") ^^ {
        case "this" => ast.Ident("this" :: Nil)
        case e: ast.Exp => e
        case other => sys.error(s"Unexpected value expression: $other")
      }
      def val_concat: Parser[List[ast.Exp]] = rep1sep(value, "++")
      def varTransform: Parser[(Option[String], List[ast.Exp])] = {
        (variable | "this" | ("(" ~> qualifiedIdent ~ "=" ~ val_concat <~ ")")) ^^ {
          case v: Variable => (None, v :: Nil)
          case "this" => (None, ast.Ident("this" :: Nil) :: Nil)
          case (v1: ast.Ident) ~ _ ~ (vc: List[ast.Exp@unchecked]) => (Option(v1.tresql), vc)
          case other => sys.error(s"Unexpected variable transform: $other")
        }
      } named "vars-transform"
      def tupleToVarTransform(t: (Option[String], List[ast.Exp])) =
        VariableTransform(ValueConcats(t._2), t._1)
      def varsTransformsOrVar: Parser[Op] = rep1sep(varTransform, "+") <~
        "$".r /*end of input*/ ^^ {
          case (None, ast.Ident("this" :: Nil) :: Nil) :: Nil => This()
          case (None, v :: Nil) :: Nil => Tresql(v.tresql)
          case vts => VariableTransforms(vts map tupleToVarTransform)
        } named "vt-or-v"
      def opWithVarsTransforms: Parser[(List[VariableTransform], Op)] = {
        def varsTransforms: Parser[VariableTransforms] =
          rep1sep(varTransform, "+") ^^
            (vts => VariableTransforms(vts map tupleToVarTransform)) named "vars-transforms"
        ((varsTransforms <~ "->") ~ op) ^^ {
          case ovts ~ op => ovts.transforms -> op
        } named "op-with-vars-transforms"
      }
      (opWithVarsTransforms |
        ((varsTransformsOrVar | op) ^^ (Nil -> _))) named "op-with-opt-vts"
    }
    def setEnvOrReturn: Parser[Step] = {
      // setenv or return regexp ends with zero width positive lookahead group
      // so that no symbol - non word character - [^\w] or space
      // is consumed but rather left to the next parser
      (("(setenv|addenv|return)(?=\\s+|[^\\w])?".r ~ opWithOptVarTransforms) ^^ {
        case cmd ~ step =>
          val (transforms, op) = step
          if (cmd == "setenv" || cmd == "addenv") SetEnv(None, transforms, op, add = cmd == "addenv")
          else Return(None, transforms, op)
      }) named "set-env-or-return"
    }

    def removeVar: Parser[RemoveVar] = ((ident | stringLiteral) <~ "-=") ^^ {
      v => RemoveVar(Option(v))
    } named "remove-var"
    def evaluation: Parser[Evaluation] =
      (opt(qualifiedIdent <~ "=") ~ opWithOptVarTransforms) ^^ {
        case variable ~ tr_op =>
          Evaluation(variable.map(_.tresql), tr_op._1, tr_op._2)
      } named "evaluation"
    def namedBlock(isBlock: Boolean): Parser[Evaluation] =
      (if (isBlock) qualifiedIdent ^^ { case n => Evaluation(Option(n.tresql), Nil, null) }
      else failure("Not block")) named "named-block"
    (removeVar | setEnvOrReturn | evaluation | namedBlock(isBlock)) named "step"
  }

  def parseOperation(op: String): Op =
    phrase(operation)(new scala.util.parsing.input.CharSequenceReader(op)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }

  // operation parsers
  def tresqlOp: MemParser[Tresql] = opt(opResultType) ~ expr ^^ { case rt ~ e =>
    val te = transformer {
      case f@ast.Fun("build_cursors",
        (o@ast.Obj(ast.Ident("this" :: Nil), _, _, _, _)) :: tail, false, None, None
      ) => f.copy(parameters = o.copy(obj = ast.Ident(viewName :: Nil)) :: tail)
    }(e)
    val dbs = traverser(dbExtractor)(Nil)(e)
    Tresql(te.tresql, dbs, rt)
  } named "tresql-op"
  def viewOp: MemParser[ViewCall] = opt(opResultType) ~ ActionRegex ~ ViewNameRegex ~ opt(operation) ^^ {
    case rt ~ action ~ view ~ op => ViewCall(action, view, op.orNull, rt)
  }  named "view-op"
  def uniqueOp: MemParser[Unique] = opt(opResultType) ~ (("unique_opt" | "unique") ~ operation) ^^ {
    case rt ~ (mode ~ op) => Unique(op, mode == "unique_opt", rt)
  } named "unique-op"
  def invocationOp: MemParser[Invocation] = Parser { in =>
    val p = opt(opResultType) ~ OpParser.InvocationRegex ~
      opt("(" ~> rep1sep(operation, ",") <~ ")") ~ opt(operation)
    p(in) match {
      case Success(rt ~ res ~ args ~ arg, next) =>
        def resolveFunction(name: String) = try {
          val (cn, fn) = classNameFunctionNameNoCheck(name, cl)
          Success(Action.Invocation(cn, fn, args.getOrElse(Nil) ++ arg.toList, rt), next)
        } catch {
          case NonFatal(_) => Failure(s"Function not found: $name", next)
        }
        resolveFunctionAliasOpt(res, cl)   // if function alias found resolve function
          .map(resolveFunction)
          .orElse(tmd.tableDefOption(res, null) // if table def found return failure - function not found
            .map(_ => Failure(s"Function not found: $res", next)))
          .getOrElse(resolveFunction(res))      // resolve function
      case e: NoSuccess => e
    }
  } named "invocation-op"
  def resourceOp: MemParser[Resource] = "resource\\s+".r ~> tresqlOp ~ opt(tresqlOp) ^^ {
    case nameTresql ~ ctTresql => Resource(nameTresql, ctTresql.orNull)
  } named "resource-op"
  def fileOp: MemParser[File] = opt(opResultType) ~
    ("file\\s+".r ~> opt("[" ~> HttpClientFileStreamerNameRegex <~ "]") ~ tresqlOp) ^^ {
    case conformTo ~ (fileStreamer ~ e) => File(e, conformTo, fileStreamer.orNull)
  } named "file-op"
  def toFileOp: MemParser[ToFile] = {
    val Filename = "filename"
    val ContentType = "content_type"
    val args = Set(Filename, ContentType)
    "to file" ~>
      opt("[" ~> HttpClientFileStreamerNameRegex <~ "]") ~ operation ~ namedOps(args) ^^ {
      case fileStreamer ~ op ~ args =>
        ToFile(op, findArg(Filename, 0, args).map(_.asInstanceOf[Tresql]).orNull,
          findArg(ContentType, 1, args).map(_.asInstanceOf[Tresql]).orNull, fileStreamer.orNull)
    } named "to-file-op"
  }
  def templateOp: MemParser[Template] = {
    val Data = "data"
    val Filename = "filename"
    val args = Set(Data, Filename)
    "template\\s+".r ~> operation ~ namedOps(args) ^^ {
      case templ ~ args =>
        args match {
          case Nil => Template(templ, null, null)
          case l =>
            val dataOp = findArg(Data, 0, l)
            val filename = findArg(Filename, 1, l).map(_.asInstanceOf[Tresql])
            Template(templ, dataOp.orNull, filename.orNull)
        }
    } named "template-op"
  }
  def emailOp: MemParser[Email] = {
    def dataOp = extractEntityOp | tresqlOp
    "email\\s+".r ~> opt("batch") ~ dataOp ~ operation ~ operation ~ rep(operation) ^^ {
      case batch ~ data ~ subj ~ body ~ att => Email(data, subj, body, att, batch.isDefined)
    } named "email-op"
  }
  def httpOp: MemParser[Http] = {
    def tu(uri: Exp) = TresqlUri.Tresql(uri.tresql)
    def http_cln = opt("[" ~> HttpClientFileStreamerNameRegex <~ "]")
    def http_no_entity: MemParser[Http] =
      opt("get" | "delete" | "head" | "options" | "trace" | "connect") ~ http_cln ~ bracesTresql ~ opt(tresqlOp) ^^ {
        case method ~ client ~ uri ~ headers =>
          Http(method.getOrElse("get"), tu(uri), headers.orNull, body = null, httpClientName = client.orNull)
      } named "http-get-delete-op"
    def http_with_entity: MemParser[Http] =
      ("post" | "put" | "patch") ~ http_cln ~ bracesTresql ~ opt(operation) ~ opt(tresqlOp) ^^ {
        case method ~ client ~ uri ~ op ~ headers =>
          Http(method, tu(uri), headers.orNull, op.orNull, httpClientName = client.orNull)
      } named "http-post-put-op"
    opt(opResultType) ~ ("(http|http_proxy)(?=\\s+)".r ~ (http_with_entity | http_no_entity)) ^^ {
      case conformTo ~ (mode ~ http) => http.copy(conformTo = conformTo, isProxy = mode == "http_proxy")
    } named "http-op"
  }
  def dbOp: MemParser[Db] = dbBlockOp ~ actionFromOp ^^ {
    case db ~ act => db.copy(action = act)
  } named "db-op"
  def dbBlockOp: MemParser[Db] = (Action.DbUseKey | Action.TransactionKey) ~ opt("[" ~> ident <~ "]") ^^ {
    case op_type ~ db => Db(null, op_type == Action.DbUseKey, db.map(AppMetadata.DbAccessKey.apply).toList)
  } named "db-block-op"
  def jsonCodecOp: MemParser[JsonCodec] = "(from|to)(?=\\s+)".r ~ "json\\s+".r ~ operation ^^ {
    case mode ~ _ ~ op => JsonCodec(mode == "to", op)
  } named "json-op"
  def confOp: MemParser[Conf] = {
    val parType = new Regex(ConfTypes.types.map(_.name).mkString("|"))
    "conf" ~> opt(parType) ~ ConfPropRegex ^^ {
      case pt ~ param => Conf(param, ConfTypes.parse(pt.orNull))
    }
  } named "conf-op"
  def httpHeaderOp: MemParser[Op] = ("extract" ~> opt("optional") <~ "header") ~ "[^:\\s]+".r ~ opt(httpOp | tresqlOp) ^^ {
    case opt ~ h ~ httpOp => HttpHeader(h, httpOp.orNull, opt.isDefined)
  } named "http-hop"
  def httpCookieOp: MemParser[Op] = ("extract" ~ "cookie") ~> ".*".r ^^ (Cookie(_)) named "http-cop"
  def extractPartsOp: MemParser[ExtractParts] =
    "extract\\s+parts".r ~> opt("[" ~> HttpClientFileStreamerNameRegex <~ "]") ^^ {
      case fs => ExtractParts(fs.orNull)
    } named "extract-parts"
  def extractEntityOp: MemParser[ExtractHttpEntity] =
    (opt(opResultType) <~ "extract\\s+entity".r) ~ opt("using" ~> ident) ~ opt(operation) ^^ {
      case conformTo ~ decoder ~ op => ExtractHttpEntity(conformTo, decoder.orNull, op.orNull)
    } named "extract-entity"
  def foreachFoldOp: MemParser[FoldOp] = ("fold" ~ "(") ~> (ident <~ ",") ~ (ident <~ ")") ~ operation ^^ {
    case res ~ el ~ op => FoldOp(res, el, op)
  } named "foreach-fold-op"
  def foreachOp: MemParser[Foreach] = foreachBlockOpBase ~ actionFromOp ~ opt(foreachFoldOp) ^^ {
    case coll ~ act ~ foldOp => Foreach(coll, act, foldOp.orNull)
  } named "foreach-op"
  def foreachBlockOpBase: MemParser[Op] = "foreach(?=\\s+|[^\\w])".r ~> operation named "foreach-block-op-base"
  def foreachBlockOp: MemParser[Foreach] = foreachBlockOpBase ~ opt(foreachFoldOp) ^^ {
    case coll ~ foldOp => Foreach(coll, null, foldOp = foldOp.orNull)
  } named "foreach-block-op"
  def ifElseOp: MemParser[If] = ifBlockOp ~ actionFromOp ~ opt(elseOp) ^^ {
      case cond ~ ifAct ~ elseOp => cond.copy(action = ifAct, elseAct = elseOp.map(_.action).orNull)
    } named "if-else-op"
  def elseOp: MemParser[Else] = elseBlockOp ~> actionFromOp ^^ (Else(_))
  def ifBlockOp: MemParser[If] = "if(?=\\s+|[^\\w])".r ~> operation ~ opt(actionFromOp <~ (elseBlockOp ~ "$".r)) ^^ {
    case cond ~ ifActElseBl => If(cond, ifActElseBl.orNull)
  } named "if-block-op"
  def elseBlockOp: MemParser[Else] = "else".r ^^^ Else(null) named "else-block-op"
  def blockOp: MemParser[BlockOp] = ifBlockOp | elseBlockOp | dbBlockOp | foreachBlockOp named "block-op"
  def thisOp: MemParser[This] = opt(opResultType) <~ "this" ^^ This.apply named "this-op"

  def bracesOp: MemParser[Op] = "(" ~> operation <~ ")" named "braces-op"
  def bracesTresql: MemParser[Exp] = (("(" ~> expr <~ ")") | expr) named "braces-tresql-op"

  def redirect: MemParser[Op] = {
    (RedirectOpRegex ~> ((RedirectToKeyRegex ^^ (s => RedirectToKey(s))) | ((setHttpHeadersOps ~ tresqlOp) ^^ {
      case hops ~ tr => Response(Tresql("303"), true, hops, tr)
    }))) named "redirect-op"
  }
  def response: MemParser[Response] = {
    val StResp = "(status|response)\\s+".r
    (StResp ~ (("ok" | "\\d+".r | variable) ~ setHttpHeadersOps ~ opt(operation))) ^? ({
      case StResp(sor) ~ (c ~ hops ~ body) =>
        val code = c match {
          case "ok" => Tresql("200")
          case v: ast.Variable => Tresql(v.tresql)
          case _ => Tresql(String.valueOf(c))
        }
        Action.Response(code, sor == "status", hops, body.orNull)
    }) named "response-op"
  }
  /* Cannot be named mem parser since depends on parameter. */
  def setOrDeleteCookie(cmd: String, mandatoryPars: Set[String] = Set()): Parser[SetHttpHeadersOp] =
    ((cmd ~ "(") ~> namedOps(allowedCookiePars, mandatoryPars, ",") <~ ")") ^? ({
      case pars if pars.forall(_._2.isInstanceOf[Tresql]) =>
        def pt =
          Tresql(pars.map { case (n, p) => s"(${p.asInstanceOf[Tresql].tresql}) $n" }.mkString("{", ", ", "}"))
        cmd match {
          case "set_cookie" => SetCookie(pt)
          case "delete_cookie" => DeleteCookie(pt)
          case x => sys.error(s"Knipis: allowed 'set_cookie' or 'delete_cookie', found: '$x'")
        }
    }, {
      case p => sys.error(s"$cmd operation must have 'name' parameteter and currently operation allows only tresql" +
        s" parameters, found: $p")
    }) named "set-or-delete-cookie"
  def setCookie: MemParser[SetCookie] =
    setOrDeleteCookie("set_cookie", Set("name", "value")) ^^ (_.asInstanceOf[SetCookie]) named "set-cookie-op"
  def deleteCookie: MemParser[DeleteCookie] =
    setOrDeleteCookie("delete_cookie") ^^ (_.asInstanceOf[DeleteCookie]) named "delete-cookie-op"
  def setHttpHeaders: MemParser[SetHttpHeaders] = (("set_headers" ~ "(") ~> tresqlOp <~ ")") ^^ {
    SetHttpHeaders(_)
  } named "set-http-headers-op"
  def setUserAttributes: MemParser[SetUserAttributes] = (("user_attrs" ~ "(") ~> tresqlOp <~ ")") ^^ {
    SetUserAttributes(_)
  } named "set-user-attributes-op"
  def setHttpHeadersOps: MemParser[List[SetHttpHeadersOp]] =
    rep(setCookie | deleteCookie | setHttpHeaders | setUserAttributes) named "set-http-headers-ops"
  def commit: MemParser[Commit.type] = "commit\\s*$".r ^^^ Commit named "commit-op"
  def rollback: MemParser[Rollback.type] = "rollback\\s*$".r ^^^ Rollback named "rollback-op"
  def operation: MemParser[Op] = (commit | rollback | redirect | response | viewOp | confOp | uniqueOp |
    httpOp | dbOp | foreachOp | ifElseOp | elseOp | resourceOp | fileOp | toFileOp | templateOp | emailOp |
    jsonCodecOp | httpHeaderOp | httpCookieOp | extractPartsOp | extractEntityOp |
    thisOp | bracesOp | invocationOp | tresqlOp) named "operation"

  private def opResultType: MemParser[OpResultType] = {
    sealed trait ResType
    case object NoType extends ResType
    case object NoBindType extends ResType
    case class ViewType(vn: String) extends ResType
    def noType: Parser[ResType] = "any" ^^^ NoType
    def nonBindableType: Parser[ResType] = "result" ^^^ NoBindType
    def viewType: Parser[ResType] = opt("`") ~> ViewNameRegex <~ opt("`") ^^ ViewType.apply

    "as" ~> ((noType | nonBindableType | viewType) ~ opt("*")) ^^ {
      case NoType ~ isColl => ViewResultType(null, isColl.nonEmpty)
      case NoBindType ~ _ => NonBindableResultType
      case ViewType(typ) ~ isColl => ViewResultType(typ, isColl.nonEmpty)
      case x => sys.error(s"Knipis, unexpected op result type: $x")
    } named "op-result-type"
  }
  private def namedOps(
    allowedNames: Set[String],
    mandatoryNames: Set[String] = Set(),
    separator: String = null
  ): Parser[List[(String, Op)]] = { // do not make mem parser since name may depend on parameters
    val namedOp: Parser[(String, Op)] =
      opt(ident <~ "=") ~ operation ^? ( {
        case Some(name) ~ op if allowedNames(name) || allowedNames.isEmpty => (name, op)
        case _ ~ op => (null, op)
      }, {
        case n ~ _ => s"Illegal argument name - $n, allowed arguments - $allowedNames"
      }) named "named-op"
    (if (separator == null) rep(namedOp) else repsep(namedOp, separator)) ^? ({
      case l if mandatoryNames.isEmpty ||
        l.size - (l.map(_._1).toSet -- mandatoryNames).size == mandatoryNames.size => l
    } , {
      case l => sys.error(s"Not all mandatory parameters (${mandatoryNames.mkString(",")}) specified - (${
        l.map(_._1).mkString(",")}), ")
    })
  } named "named-ops"

  private def findArg(name: String, idx: Int, l: List[(String, Op)]) =
    l.find(_._1 == name).orElse(l.lift(idx).filter(_._1 == null)).map(_._2)

  def actionFromOp: MemParser[Action] = new Parser[Action] {
    def apply(in: Input): ParseResult[Action] = {
      val start = in.offset
      operation(in).flatMapWithNext(op => next =>
        Success(Action((Evaluation(None, Nil, op), in.source.subSequence(start, next.offset).toString.trim) :: Nil),
          next)
      )
    }
  }
}

object OpParser extends Loggable {
  val InvocationRegex = """(?U)\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*(\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)*""".r
}
object AppMetadata extends Loggable {

  sealed trait RequestDecoder
  case object DefaultDecoder extends RequestDecoder
  case object NoneDecoder extends RequestDecoder
  case class CustomDecoder(className: String, function: String) extends RequestDecoder

  case class AuthFilters(
    forGet: Seq[String],
    forList: Seq[String],
    forInsert: Seq[String],
    forUpdate: Seq[String],
    forDelete: Seq[String]
  )

  case class FilterLabel(fieldName: String, filterName: String)
  case class FilterParameter(
    name: String, table: String, label: FilterLabel,
    nullable: Boolean, required: Boolean,
    type_ : Type, enum_ : Seq[String],
    refViewName: String, filterType: FilterType,
  )

  val AuthEmpty = AuthFilters(Nil, Nil, Nil, Nil, Nil)

  val JoinsCompilerCacheName  = "joins-compiler-cache.cbor"
  def loadJoinsParserCache(getResourceAsStream: String => InputStream): Map[String, Map[String, Exp]] = {
    val res = getResourceAsStream(s"/$JoinsCompilerCacheName")
    if (res == null) {
      logger.debug(s"No joins compiler cache resource - '/$JoinsCompilerCacheName' found")
      Map()
    } else {
      import io.bullet.borer._
      import CacheIo.expCodec
      val cache =
        Cbor.decode(res).to[Map[String, Map[String, Exp]]].value
      logger.debug(s"Joins compiler cache loaded for databases: ${
        cache.map { case (db, c) => s"$db - ${c.size}" }.mkString("(", ", ", ")") }")
      cache
    }
  }

  val ViewNameToQueryVariablesCacheName  = "view-query-variables-cache.cbor"
  private def loadViewNameToQueryVariablesCache(classLoader: ClassLoader): Map[String, Seq[ast.Variable]] = {
    val res = Option(classLoader).getOrElse(getClass.getClassLoader)
      .getResourceAsStream(ViewNameToQueryVariablesCacheName)
    if (res == null) {
      logger.debug(s"Query variables cache resource not found: '$ViewNameToQueryVariablesCacheName'")
      Map()
    } else {
      import io.bullet.borer._
      import CacheIo.varCodec
      val cache =
        Cbor.decode(res).to[Map[String, Seq[ast.Variable]]].value
      logger.debug(s"Query variables cache loaded for ${cache.size} views")
      cache
    }
  }

  def joinsParserCacheFactory(joinsParserCache: Map[String, Map[String, Exp]], cacheSize: Int)(db: String): Option[Cache] = {
    joinsParserCache.get(Option(db).getOrElse("null")).map { data =>
      val cache = new SimpleCache(cacheSize)
      cache.load(data)
      cache
    }.orElse(Some(new SimpleCache(cacheSize)))
  }

  def joinsParserCacheFactory(getResourceAsStream: String => InputStream, cacheSize: Int)(db: String): Option[Cache] =
    joinsParserCacheFactory(loadJoinsParserCache(getResourceAsStream), cacheSize)(db)

  val JobCall = "call"

  object Action {
    val Get    = "get"
    val List   = "list"
    val Save   = "save"
    val Insert = "insert"
    val Update = "update"
    val Upsert = "upsert"
    val Delete = "delete"
    val Create = "create"
    val Count  = "count"
    val Job    = "job"
    val Head   = "head"
    val Options= "options"
    val Post   = "post"
    val Put    = "put"
    val UpdatePlus = "update+" // Update with key update - post to old key uri, new key in body
    def apply() =
      Set(Get, List, Save, Insert, Update, Upsert, Delete, Create, Count, Job, Head, Options, Post, Put, UpdatePlus)

    val ValidationsKey = "validations"
    val DbUseKey = "db use"
    val TransactionKey = "transaction"
    val OffsetKey = "offset"
    val LimitKey  = "limit"
    val OrderKey  = "sort"

    val allowedCookiePars =
      Set("name", "value", "expires", "max_age", "domain", "path", "secure", "http_only", "extension")

    object ConfTypes {
      def types: Set[ConfType] = Set(NumberConf, StringConf, BooleanConf)
      def parse(name: String): ConfType = types.find(_.name == name).getOrElse {
        if (name == null) null
        else throw new IllegalArgumentException(s"Wrong name: '$name', supported names: '${
          types.map(_.name).mkString(", ")}'")
      }
    }
    sealed trait ConfType { def name: String }
    case object NumberConf extends ConfType { def name = "number" }
    case object StringConf extends ConfType { def name = "string" }
    case object BooleanConf extends ConfType { def name = "boolean" }

    sealed trait SetHttpHeadersOp { def tresql: Tresql }
    case class SetHttpHeaders(tresql: Tresql) extends SetHttpHeadersOp
    /** For argument names see: {{{https://pekko.apache.org/api/pekko-http/current/org/apache/pekko/http/scaladsl/model/headers/HttpCookie$.html}}} */
    case class SetCookie(tresql: Tresql) extends SetHttpHeadersOp
    case class DeleteCookie(tresql: Tresql) extends SetHttpHeadersOp
    case class SetUserAttributes(tresql: Tresql) extends SetHttpHeadersOp

    sealed trait Op
    sealed trait OpResultType
    sealed trait BlockOp extends Op {
      def action: Action
    }
    sealed trait CastableOp extends Op {
      def conformTo: Option[OpResultType]
    }
    sealed trait Step {
      def name: Option[String]
    }

    case class VariableTransform(from: ValueConcats, to: Option[String] = None)
    case class ValueConcats(vals: List[ast.Exp])
    case class ViewResultType(viewName: String = null, isCollection: Boolean = false) extends OpResultType
    case object NonBindableResultType extends OpResultType
    case class FoldOp(resVar: String, elVar: String, op: Op)

    case class Tresql(tresql: String,
                      dbs: List[ast.Db] = Nil,
                      conformTo: Option[OpResultType] = None) extends CastableOp
    case class ViewCall(method: String, view: String, data: Op = null, conformTo: Option[OpResultType] = None) extends Op
    case class RedirectToKey(name: String) extends Op
    case class Unique(innerOp: Op, opt: Boolean, conformTo: Option[OpResultType] = None) extends CastableOp
    case class Invocation(className: String,
                          function: String,
                          args: List[Op] = Nil,
                          conformTo: Option[OpResultType] = None) extends CastableOp
    case class Response(
      codeTresql: Tresql,
      statusMode: Boolean, // if status mode = true, body op must be tresql and is executed as unique[String]
      setHttpHeaders: List[SetHttpHeadersOp] = Nil,
      body: Op = null,
    ) extends Op
    case class VariableTransforms(transforms: List[VariableTransform]) extends Op
    case class Foreach(initOp: Op, action: Action, foldOp: FoldOp = null) extends BlockOp
    case class If(cond: Op, action: Action, elseAct: Action = null) extends BlockOp
    case class Resource(nameTresql: Tresql, contentTypeTresql: Tresql = null) extends Op
    case class File(
      idShaTresql: Tresql,
      conformTo: Option[OpResultType] = None,
      fileStreamerName: String = null
    ) extends CastableOp
    case class ToFile(
      contentOp: Op,
      nameTresql: Tresql = null,
      contentTypeTresql: Tresql = null,
      fileStreamerName: String = null,
    ) extends Op
    case class Template(template: Op, dataOp: Op = null, filenameTresql: Tresql = null) extends Op
    case class Email(recipients: Op, subject: Op, body: Op, attachmentsOp: List[Op] = Nil, isBatch: Boolean = false) extends Op
    case class Http(method: String,
                    uriTresql: TresqlUri.Tresql,
                    headerTresql: Tresql = null,
                    body: Op = null,
                    conformTo: Option[OpResultType] = None,
                    httpClientName: String = null,
                    isProxy: Boolean = false) extends CastableOp
    case class HttpHeader(name: String, httpOp: Op = null, isOpt: Boolean = false) extends Op
    case class Cookie(name: String) extends Op
    case class ExtractHttpEntity(conformTo: Option[OpResultType] = None, decoder: String = null, op: Op = null) extends Op
    /** This op can be used if view property 'decode request' is false, for multipart request it extracts parts,
     * for simple request creates one part with body as a Source.
     * File streamer name indicates which file streamer to use for parts serialization.
     * */
    case class ExtractParts(fileStreamerName: String = null) extends Op
    case class Db(action: Action, doRollback: Boolean, dbs: List[DbAccessKey]) extends BlockOp
    case class Conf(param: String, paramType: ConfType = null) extends Op
    case class JsonCodec(encode: Boolean, op: Op) extends Op
    /** This operation exists only in parsing stage for if operation */
    case class Else(action: Action) extends BlockOp
    case class Block(action: Action) extends BlockOp
    case object Commit extends Op
    case object Rollback extends Op

    case class This(conformTo: Option[OpResultType] = None) extends Op
    /**
     * @param name - optional variable name i.e. variable = ...
     * @param varTrans - variable transformation for operation
     * @param op - step operation
     * */
    case class Evaluation(name: Option[String], varTrans: List[VariableTransform], op: Op) extends Step
    case class SetEnv(name: Option[String], varTrans: List[VariableTransform], value: Op, add: Boolean = false) extends Step
    case class Return(name: Option[String], varTrans: List[VariableTransform], value: Op) extends Step
    case class Validations(name: Option[String], validations: Seq[String], db: Option[DbAccessKey]) extends Step
    case class RemoveVar(name: Option[String]) extends Step

    type OpTraverser[T] = T => PartialFunction[Op, T]
    type StepTraverser[T] = T => PartialFunction[Step, T]

    def opTraverser[T](opTrav: => OpTraverser[T], stepTrav: => StepTraverser[T])(
        extractor: OpTraverser[T]): OpTraverser[T] = {
      def traverse(state: T): PartialFunction[Op, T] = {
        case _: Tresql | _: RedirectToKey | _: Response |
             _: VariableTransforms | _: File | _: Conf | _: Cookie |
             _: ExtractParts | _: This | _: Resource | Commit | Rollback | null => state
        case o: ViewCall => opTrav(state)(o.data)
        case Unique(o, _, _) => opTrav(state)(o)
        case Foreach(o, a, foldOp) =>
          val ns = traverseAction(a)(stepTrav)(opTrav(state)(o))
          if (foldOp == null) ns else opTrav(ns)(foldOp.op)
        case If(o, a, e) =>
          val r = traverseAction(a)(stepTrav)(opTrav(state)(o))
          if (e == null) r else traverseAction(e)(stepTrav)(r)
        case o: ToFile => opTrav(state)(o.contentOp)
        case o: Template => opTrav(state)(o.dataOp)
        case Email(r, s, b, a, _) => a.foldLeft(opTrav(opTrav(opTrav(state)(r))(s))(b))(opTrav(_)(_))
        case o: Http => opTrav(state)(o.body)
        case h: HttpHeader => if (h.httpOp == null) state else opTrav(state)(h.httpOp)
        case Db(a, _, _) => traverseAction(a)(stepTrav)(state)
        case Block(a) => traverseAction(a)(stepTrav)(state)
        case JsonCodec(_, o) => opTrav(state)(o)
        case i: Invocation => i.args.foldLeft(state)(opTrav(_)(_))
        case ExtractHttpEntity(_, _, o) => opTrav(state)(o)
      }
      state => extractor(state) orElse traverse(state)
    }

    def stepTraverser[T](opTrav: => OpTraverser[T])(extractor: StepTraverser[T]): StepTraverser[T] = {
      def traverse(state: T): PartialFunction[Step, T] = {
        case _: Validations | _: RemoveVar => state
        case s: Evaluation => opTrav(state)(s.op)
        case s: SetEnv => opTrav(state)(s.value)
        case s: Return => opTrav(state)(s.value)
      }
      state => extractor(state) orElse traverse(state)
    }

    def traverseAction[T](action: Action)(stepTraverser: StepTraverser[T]): T => T =
      action.steps.map(_._1).foldLeft(_) { stepTraverser(_)(_) }

    object TresqlExtraction {
      type ViewExtractor[T] = T => ViewDef => T
      type TresqlExtractor[T] = T => Tresql => T
      type StepTresqlTraverser[T] = StepTraverser[State[T]]
      type OpTresqlTraverser[T] = OpTraverser[State[T]]

      case class State[T](
        action: String, name: String,
        viewDefs: Map[String, ViewDef],
        tresqlExtractor: TresqlExtractor[T],
        viewExtractor: ViewExtractor[T],
        processed: Set[(String, String)],
        value: T
      )

      def processView[T](stepTresqlTrav: => StepTresqlTraverser[T])(s: State[T]): State[T] = {
        if (s.processed(s.action -> s.name)) s else {
          def process(vn: String, initVal: T, processed: Set[String]): T = {
            if (processed(vn)) initVal else {
              val vd = s.viewDefs(vn)
              val newVal = s.viewExtractor(initVal)(vd)
              vd.fields
                .collect { case f if f.type_.isComplexType => f.type_.name }
                .foldLeft(newVal -> (processed + vn)) {
                  case ((res, pr), fName) => process(fName, res, pr) -> (pr + fName)
                }._1
            }
          }

          val newVal = process(s.name, s.value, Set())
          val s1 = s.copy(value = newVal, processed = s.processed + (s.action -> s.name))
          val vd = s1.viewDefs(s.name)
          vd.actions.get(s1.action).map { a =>
            traverseAction(a)(stepTresqlTrav)(s1)
          }.getOrElse(s1)
        }
      }

      def opTresqlTraverser[T](opTresqlTrav: => OpTresqlTraverser[T],
        stepTresqlTrav: => StepTresqlTraverser[T])(extractor: OpTresqlTraverser[T]):
          OpTresqlTraverser[T] = {
        def traverse(state: State[T]): PartialFunction[Op, State[T]]= {
          val nv: T => Tresql => T = v => t => Option(t).map(state.tresqlExtractor(v)(_)).getOrElse(v)
          def opTrTr = opTresqlTrav(state)
          def us(s: State[T], v: T) = { s.copy(value = v) }
          {
            case t: Tresql => us(state, nv(state.value)(t))
            case Response(codeTresql, _, hops, body) =>
              val s = us(state, nv(state.value)(codeTresql))
              hops.foldLeft(opTresqlTrav(s)(body)){ (resSt, hdop) =>
                us(resSt, nv(resSt.value)(hdop.tresql))
              }
            case Resource(nameTresql, contentTypeTresql) =>
              val s1 = us(state, nv(state.value)(nameTresql))
              us(s1, nv(s1.value)(contentTypeTresql))
            case File(idShaTresql, _, _) => us(state, nv(state.value)(idShaTresql))
            case ToFile(contentOp, nameTresql, contentTypeTresql, _) =>
              val s1 = opTrTr(contentOp)
              val s2 = us(s1, nv(s1.value)(nameTresql))
              us(s2, nv(s2.value)(contentTypeTresql))
            case Template(template, dataOp, filenameTresql) =>
              val s = opTresqlTrav(opTresqlTrav(state)(template))(dataOp)
              us(s, nv(s.value)(filenameTresql))
            case Email(r, s, b, a, _) =>
              a.foldLeft(
                opTresqlTrav(opTresqlTrav(opTresqlTrav(state)(r))(s))(b)
              )(opTresqlTrav(_)(_))
            case Http(_, uriTresql, headerTresql, body, _, _, _) =>
              val s1 = us(state, nv(state.value)(Tresql(uriTresql.uriTresql)))
              val s2 = us(s1, nv(s1.value)(headerTresql))
              opTresqlTrav(s2)(body)
            case HttpHeader(_, httpOp, _) => opTresqlTrav(state)(httpOp)
            case ViewCall(method, view, data, _) =>
              val vn = if (view == "this") state.name else view
              val ns = opTrTr(data)
              processView(stepTresqlTrav)(ns.copy(action = method, name = vn))
            case Invocation(_, _, o, _) => o.foldLeft(state)(opTresqlTrav(_)(_))
            case ExtractHttpEntity(_, _, o) => opTrTr(o)
          }
        }
        opTraverser(opTresqlTrav, stepTresqlTrav) { state => extractor(state) orElse traverse(state) }
      }

      def stepTresqlTraverser[T](opTresqlTrav: => OpTresqlTraverser[T])(
        extractor: StepTresqlTraverser[T]): StepTresqlTraverser[T] = {
        def traverse(state: State[T]): PartialFunction[Step, State[T]] = {
          {
            case Validations(_, validations, _) =>
              val nv = validations.foldLeft(state.value) { (v, s) =>
                state.tresqlExtractor(v)(Tresql(s))
              }
              state.copy(value = nv)
          }
        }
        stepTraverser(opTresqlTrav)(state => extractor(state) orElse traverse(state))
      }
    }
  }

  case class Action(steps: List[(Action.Step, String)])

  /** Database name (as used in mojoz metadata) and corresponding connection pool name */
  case class DbAccessKey(
    db: String,
  )

  trait AppViewDefExtras {
    val limit: Int
    val paths: Seq[String]
    val explicitDb: Boolean
    val decoder: RequestDecoder
    val maxContentSize: jLong
    val timeout: FiniteDuration
    val sqlTimeout: FiniteDuration
    val auth: AuthFilters
    val apiMethodToRoles: Map[String, Set[String]]
    val actions: Map[String, Action]
    val actionToDbAccessKeys: Map[String, Seq[DbAccessKey]]
    val minKeySizeForCollection: Int
    val maxKeySizeForCollection: Int
  }

  private [wabase] case class AppViewDef(
    limit: Int = 1000,
    paths: Seq[String] = Nil,
    explicitDb: Boolean = false,
    decoder: RequestDecoder = DefaultDecoder,
    maxContentSize: jLong = null,
    timeout: FiniteDuration = null,
    sqlTimeout: FiniteDuration = null,
    auth: AuthFilters = AuthFilters(Nil, Nil, Nil, Nil, Nil),
    apiMethodToRoles: Map[String, Set[String]] = Map(),
    actions: Map[String, Action] = Map(),
    actionToDbAccessKeys: Map[String, Seq[DbAccessKey]] = Map.empty,
    minKeySizeForCollection: Int = 0,
    maxKeySizeForCollection: Int = 0,
  ) extends AppViewDefExtras

  case class FieldApiOps(
    insertable: Boolean,
    updatable: Boolean,
    excluded: Boolean,
  ) {
    val readonly = !insertable && !updatable || excluded
  }

  trait AppFieldDefExtras {
    val api: FieldApiOps
    val label: String
    val required: Boolean
    val sortable: Boolean
    val visible: Boolean
  }

  private [wabase] case class AppFieldDef(
    api:  FieldApiOps = FieldApiOps(insertable = false, updatable = false, excluded = false),
    label:     String = null,
    required: Boolean = false,
    sortable: Boolean = false,
    visible:  Boolean = true,
  ) extends AppFieldDefExtras

  val WabaseViewExtrasKey = "wabase-view-extras"
  val WabaseFieldExtrasKey = "wabase-field-extras"
  implicit class AugmentedAppViewDef(viewDef: ViewDef)
         extends QuereaseMetadata.AugmentedQuereaseViewDef(viewDef)
            with AppViewDefExtras {
    private val defaultExtras = AppViewDef()
    private val appExtras = extras(WabaseViewExtrasKey, defaultExtras)
    override val limit = appExtras.limit
    override val paths = appExtras.paths
    override val explicitDb = appExtras.explicitDb
    override val decoder = appExtras.decoder
    override val maxContentSize = appExtras.maxContentSize
    override val timeout = appExtras.timeout
    override val sqlTimeout = appExtras.sqlTimeout
    override val auth = appExtras.auth
    override val apiMethodToRoles = appExtras.apiMethodToRoles
    override val actions = appExtras.actions
    override val actionToDbAccessKeys = appExtras.actionToDbAccessKeys
    override val minKeySizeForCollection = appExtras.minKeySizeForCollection
    override val maxKeySizeForCollection = appExtras.maxKeySizeForCollection
    def updateWabaseExtras(updater: AppViewDef => AppViewDef): ViewDef =
      updateExtras(WabaseViewExtrasKey, updater, defaultExtras)

    override protected def updateExtrasMap(extras: Map[String, Any]) = viewDef.copy(extras = extras)
    override protected def extrasMap = viewDef.extras
  }
  implicit class AugmentedAppFieldDef(fieldDef: FieldDef)
         extends QuereaseMetadata.AugmentedQuereaseFieldDef(fieldDef)
            with AppFieldDefExtras {
    private val defaultExtras = AppFieldDef()
    val appExtras = extras(WabaseFieldExtrasKey, defaultExtras)
    override val api = appExtras.api
    override val label = appExtras.label
    override val required = appExtras.required
    override val sortable = appExtras.sortable
    override val visible = appExtras.visible
    def updateWabaseExtras(updater: AppFieldDef => AppFieldDef): FieldDef =
      updateExtras(WabaseFieldExtrasKey, updater, defaultExtras)

    override protected def updateExtrasMap(extras: Map[String, Any]): Any = fieldDef.copy(extras = extras)
    override protected def extrasMap = fieldDef.extras
  }

  case class PathNameAndParameters(
    name: String,
    parameters: Seq[PathParameter]
  )

  case class PathParameter(
    name: String,
    typeName: String,
    pattern: String,
  )

  case class RouteDef(
    methods: Set[HttpMethod],
    path: Regex,
    requestHandler: Action.Invocation,
    errorHandler: Action.Invocation,
    pathNamesAndParameters: Seq[PathNameAndParameters],
    extras: Map[String, Any],
  )

  trait AppMdConventions extends MdConventions {
    def isIntegerName(name: String) = false
    def isDecimalName(name: String) = false
  }

  import MdConventions._
  class DefaultAppMdConventions(resourceLoader: String => InputStream)(
    integerNamePatternStrings: Seq[String] =
      namePatternsFromResource("/md-conventions/integer-name-patterns.txt", Nil, resourceLoader),
    decimalNamePatternStrings: Seq[String] =
      namePatternsFromResource("/md-conventions/decimal-name-patterns.txt", Nil, resourceLoader),
  ) extends SimplePatternMdConventions(resourceLoader) with AppMdConventions {

  def this() = this(classOf[DefaultAppMdConventions].getResourceAsStream _)()

  val integerNamePatterns = integerNamePatternStrings.map(pattern).toSeq
  val decimalNamePatterns = decimalNamePatternStrings.map(pattern).toSeq

  override def isIntegerName(name: String) =
    integerNamePatterns exists matches(name)
  override def isDecimalName(name: String) =
    decimalNamePatterns exists matches(name)
  }

  object AppConstraintNamingRules extends SimpleConstraintNamingRules {
    override val maxNameLen = 63 // default maximum identifier length on postgres
  }

  object KnownAuthOps {
    val Get = "get"
    val List = "list"
    val Save = "save"
    val Insert = "insert"
    val Update = "update"
    val Delete = "delete"
    def apply() =
      Set(Get, List, Save, Insert, Update, Delete)
  }

  object KnownViewExtras {
    val Api = "api"
    val Auth = "auth"
    val Key   = "key"
    val Limit = "limit"
    val Paths = "paths"
    val Validations = "validations"
    val ExplicitDb = "explicit db"
    val Decoder = "decoder"
    val Timeout = "timeout"
    val SqlTimeout = "sql-timeout"
    val Swagger = "swagger"
    val QuereaseViewExtrasKey = QuereaseMetadata.QuereaseViewExtrasKey
    val WabaseViewExtrasKey = AppMetadata.WabaseViewExtrasKey
    def apply() =
      Set(Api, Auth, Key, Limit, Paths, Validations, ExplicitDb,
          Decoder, Timeout, SqlTimeout, Swagger, QuereaseViewExtrasKey, WabaseViewExtrasKey,
      ) ++
        Action()
  }
  object KnownFieldExtras {
    val FieldApi = "field api" // avoid name clash with "api"

    val Excluded = "excluded"
    val Readonly = "readonly"
    val Readwrite = "readwrite"
    val NoInsert = "no insert"
    val NoUpdate = "no update"

    val Domain = "domain" // legacy, not ported, ignored
    val Required = "required"
    val Sortable = "sortable"
    val Hidden = "hidden"
    val Visible = "visible"
    val Initial = "initial"
    val Swagger = "swagger"
    val QuereaseFieldExtrasKey = QuereaseMetadata.QuereaseFieldExtrasKey
    val WabaseFieldExtrasKey = AppMetadata.WabaseFieldExtrasKey
    def apply() = Set(
      Domain, Hidden, Sortable, Visible, Required,
      FieldApi, Initial, Swagger, QuereaseFieldExtrasKey, WabaseFieldExtrasKey)
  }


  object ViewDefExtrasUtils {
    def getStringSeq(name: String, extras: Map[String, Any]): Seq[String] = {
      getSeq(name, extras) map {
        case s: java.lang.String => s
        case m: java.util.Map[_, _] =>
          if (m.size == 1) m.entrySet.asScala.toList.head.getKey.toString
          else m.toString // TODO error?
        case x => x.toString
      }
    }
    def getSeq(name: String, extras: Map[String, Any]): Seq[_] =
      Option(extras).flatMap(_ get name) match {
        case Some(s: java.lang.String) => Seq(s)
        case Some(a: java.util.ArrayList[_]) => a.asScala.toList
        case None => Nil
        case Some(null) => Seq("")
        case Some(x) => Seq(x)
      }
    def getIntExtra(name: String, viewDef: ViewDef) =
      Option(viewDef.extras).flatMap(_ get name).map {
        case i: Int => i
        case x => sys.error(
          s"Expecting int value, viewDef, key: ${viewDef.name}, $name")
      }
    def getStringExtra(name: String, viewDef: ViewDef) =
      Option(viewDef.extras).flatMap(_ get name).map {
        case s: String => s
        case x => sys.error(
          s"Expecting string value, viewDef, key: ${viewDef.name}, $name")
      }
    def getBooleanExtraOpt(name: String, viewDef: ViewDef) =
      Option(viewDef.extras).flatMap(_ get name).map {
        case b: Boolean => b
        case s: String if s == name => true
        case x => sys.error(
          s"Expecting boolean value or no value, viewDef, key: ${viewDef.name}.$name")
      }
    def getBooleanExtra(name: String, viewDef: ViewDef) =
      getBooleanExtraOpt(name, viewDef) getOrElse false

    def toAuth(viewDef: ViewDef, authPrefix: String, knownAuthOps: Set[String]) = {
      import KnownAuthOps._
      viewDef.extras.keySet
        .filter(k => k == authPrefix || k.startsWith(authPrefix + " "))
        .foldLeft(AuthEmpty)((a, k) => {
          val filters = getStringSeq(k, viewDef.extras)
          if (k == authPrefix)
            a.copy(
              forGet = a.forGet ++ filters,
              forList = a.forList ++ filters,
              forInsert = a.forInsert ++ filters,
              forUpdate = a.forUpdate ++ filters,
              forDelete = a.forDelete ++ filters)
          else {
            val ops = k.substring(authPrefix.length).trim.split("[\\s,]+").toList.filter(_ != "")
            val unknownAuthOps = ops.toSet -- knownAuthOps
            if (unknownAuthOps.nonEmpty)
              sys.error(
                s"Unknown auth specifier(s), viewDef: ${viewDef.name}, specifier(s): ${unknownAuthOps.mkString(", ")}")
            ops.foldLeft(a)((a, op) => op match {
              case Get =>
                a.copy(forGet = a.forGet ++ filters)
              case List => a.copy(forList = a.forList ++ filters)
              case Save => a.copy(
                forInsert = a.forInsert ++ filters,
                forUpdate = a.forUpdate ++ filters)
              case Insert => a.copy(forInsert = a.forInsert ++ filters)
              case Update => a.copy(forUpdate = a.forUpdate ++ filters)
              case Delete => a.copy(forDelete = a.forDelete ++ filters)
            })
          }
        })
    }
  }

  object FieldDefExtrasUtils {
    def fieldNameToLabel(n: String) =
      n.replace("_", " ").capitalize
    def fieldLabelFromName(f: FieldDef) = fieldNameToLabel(f.fieldName)
    def getExtraOpt(viewDef: ViewDef, f: FieldDef, key: String) =
      Option(f.extras).flatMap(_ get key).map {
        case s: String => s
        case i: Int => i.toString
        case l: Long => l.toString
        case d: Double => d.toString
        case bd: BigDecimal => bd.toString
        case b: Boolean => b.toString
        case null => null
        case x => sys.error(
          s"Expecting String, AnyVal, BigDecimal value or no value, viewDef field, key: ${viewDef.name}.${f.name}, $key")
      }
    def getBooleanExtraOpt(viewDef: ViewDef, f: FieldDef, key: String) =
      Option(f.extras).flatMap(_ get key).map {
        case b: Boolean => b
        case s: String if s == key => true
        case x => sys.error(
          s"Expecting boolean value or no value, viewDef field, key: ${viewDef.name}.${f.name}, $key")
      }
    def getBooleanExtra(viewDef: ViewDef, f: FieldDef, key: String) =
      getBooleanExtraOpt(viewDef, f, key) getOrElse false
  }

  object ActionCache {
    val QuereaseActionCacheName = "querease-action-cache.cbor"
    import io.bullet.borer._
    import CacheIo.actionCodec

    def loadSerializedCache(getResourceAsStream: String => InputStream): Map[String, Action] = {
      val res = getResourceAsStream(s"/$QuereaseActionCacheName")
      if (res == null) {
        logger.debug(s"No querease view action cache resource - '/$QuereaseActionCacheName' found")
        Map()
      } else {
        val cache =
          Cbor.decode(res).to[Map[String, Action]].value
        logger.debug(s"Querease action cache loaded for ${cache.size} views.")
        cache
      }
    }

    def serializeCache(cache: CacheBase[Action]): Map[String, Array[Byte]] = {
      Map(QuereaseActionCacheName -> Cbor.encode(cache.toMap).toByteArray)
    }

    def createCache(initData: Map[String, Action], maxSize: Int): CacheBase[Action] = {
      val cache = new SimpleCacheBase[Action](maxSize, "Action cache")
      cache.load(initData)
      cache
    }
  }

  import org.apache.pekko.http.scaladsl.server.PathMatcher
  import org.apache.pekko.http.scaladsl.server.PathMatchers._
  /**
   * Path matcher can be used to extract fragments from swagger section in route or view definitions
   * */
  def swaggerPathToPathMatcher(path: String): PathMatcher[Unit] = {
    val isParam = """\{[^}]+\}""".r
    val swaggerSegments = path.stripPrefix("/").split("/", -1)  // negative second parameter ensures empty string(s) at the end of array if path ends with slash(es)
    swaggerSegments.foldLeft[PathMatcher[Unit]](Neutral) {
      (pm, seg) =>
        if (isParam.pattern.matcher(seg).matches()) (pm / Segment).tmap(_ => ())
        else (pm / seg).tmap(_ => ())
    }
  }
  def isFullMatch(path: Uri.Path, matcher: PathMatcher[Unit]): Boolean = {
    matcher(path) match {
      case m: PathMatcher.Matched[_] => m.pathRest.isEmpty
      case _ => false
    }
  }

  def sha256(data: Any): String = {
    java.security.MessageDigest.getInstance("SHA-256")
      .digest(ResultEncoder.encodeAnyToJsonBytes(data))
      .map("%02x".format(_)).mkString
  }

}
