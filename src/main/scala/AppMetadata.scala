package org.wabase

import org.mojoz.metadata.ViewDef
import org.mojoz.metadata.FieldDef
import org.mojoz.metadata.in._
import org.mojoz.metadata.io.MdConventions
import org.mojoz.metadata.out.DdlGenerator.SimpleConstraintNamingRules
import org.mojoz.querease.QuereaseExpressions.DefaultParser
import org.mojoz.querease.QueryStringBuilder.CompilationUnit
import org.mojoz.querease.{QuereaseExpressions, QuereaseMetadata, TresqlJoinsParser, TresqlMetadata}
import org.tresql.{Cache, MacroResourcesImpl, QueryParser, SimpleCache, SimpleCacheBase, ast}
import org.tresql.ast.{Exp, Variable}
import org.tresql.parsing.QueryParsers
import org.wabase.AppMetadata.{Action, JobAct}
import org.wabase.AppMetadata.Action.TresqlExtraction.{OpTresqlTraverser, State, StepTresqlTraverser, opTresqlTraverser, stepTresqlTraverser}
import org.wabase.AppMetadata.Action.{Validations, VariableTransform, VariableTransforms, ViewCall, traverseAction}

import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.{Map, Seq, Set}
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls
import scala.util.Try
import scala.util.matching.Regex

trait AppMetadata extends QuereaseMetadata { this: AppQuerease =>

  import AppMetadata._

  val knownApiMethods = Set("create", "count", "get", "list", "insert", "update", "save", "delete")
  lazy val knownViewExtras = KnownViewExtras()
  lazy val knownPrefixes = Set(KnownViewExtras.Auth)
  val knownAuthOps = KnownAuthOps()
  lazy val knownFieldExtras = KnownFieldExtras()

  lazy val defaultCpName = TresqlResourcesConf.DefaultCpName

  /** Get macro class from 'main' tresql resources config */
  override lazy val macrosClass: Class[_] =
    // somehow flatMap needs type parameter [Class[_]] for scala 2.12.x compiler in order to succeed
    TresqlResourcesConf.confs.get(null)
      .flatMap[Class[_]](c => Option(c.macrosClass))
      .getOrElse(classOf[Macros])
  override lazy val joinsParser: JoinsParser =
    new TresqlJoinsParser(
      // to avoid stack overflow cannot use directly field tresqlMetadata,
      // since it is initialized with viewDefs (for cursor table metadata)
      TresqlMetadata(tableMetadata.tableDefs, typeDefs, macrosClass, resourceLoader, aliasToDb),
      createJoinsParserCache(_)
    )
  override lazy val metadataConventions: AppMdConventions = new DefaultAppMdConventions(resourceLoader)()
  override lazy val nameToViewDef: Map[String, ViewDef] =
    toAppViewDefs(viewDefLoader.nameToViewDef)

  private val actionParser: String => String => Map[String, Any] => Action =
    objectName => dataKey => dataMap => {
      val opParser = new OpParser(objectName, tresqlUri, opParserCache(objectName))
      parseAction(objectName, ViewDefExtrasUtils.getSeq(dataKey, dataMap), opParser)
    }
  private def validateDuplicateNames = {
    val viewNames = viewDefLoader.nameToViewDef.keys.toSet
    val jobNames = jobDefLoader.nameToJobDef.keys.toSet
    val routeNames = routeDefLoader.routeDefs.map(_.name).toSet
    val duplicates = viewNames intersect jobNames intersect routeNames
    require(duplicates.isEmpty, s"Duplicate view, job, route names found - '${duplicates.mkString(", ")}'")
  }

  lazy val jobDefLoader = new YamlJobDefLoader(yamlMetadata, actionParser)
  lazy val nameToJobDef: Map[String, JobDef] = {
    validateDuplicateNames
    transformJobDefs(jobDefLoader.nameToJobDef)
  }

  lazy val routeDefLoader =
    new YamlRouteDefLoader(yamlMetadata, actionParser)
  lazy val routeDefs: Seq[RouteDef] = {
    validateDuplicateNames
    routeDefLoader.routeDefs
  }

  protected lazy val actionOpCache: scala.collection.concurrent.Map[String, OpParser.Cache] = {
    val m = new java.util.concurrent.ConcurrentHashMap[String, OpParser.Cache]
    m.putAll(OpParser.loadSerializedOpCaches(resourceLoader).transform { (_, data) =>
      OpParser.createOpParserCache(data, parserCacheSize)
    }.asJava)
    m.asScala
  }

  protected def opParserCache(name: String) = actionOpCache.getOrElseUpdate(
    name,
    OpParser.createOpParserCache(Map(), parserCacheSize)
  )
  protected lazy val joinsParserCache: Map[String, Map[String, Exp]] =
    loadJoinsParserCache(resourceLoader)
  lazy val viewNameToQueryVariablesCache: Map[String, Seq[ast.Variable]] =
    loadViewNameToQueryVariablesCache(resourceLoader)


  def jobDefOption(jobName: String): Option[JobDef] = nameToJobDef.get(jobName)
  def jobDef(jobName: String): JobDef = jobDefOption(jobName)
    .getOrElse(sys.error(s"Job definition for $jobName not found"))


  def toAppViewDefs(mojozViewDefs: Map[String, ViewDef]) = transformAppViewDefs {
    val inlineViewDefNames =
      mojozViewDefs.values.flatMap { viewDef =>
        viewDef.fields.filter { field =>
          field.type_.isComplexType &&
          field.type_.name == viewDef.name + "_" + field.name // XXX
        }.map(_.type_.name)
      }.toSet
    mojozViewDefs.transform { (_, v) => toAppViewDef(v, isInline = inlineViewDefNames.contains(v.name)) }
  }
  override def viewNameFromMf[T <: AnyRef](implicit mf: Manifest[T]): String =
    classToViewNameMap.getOrElse(mf.runtimeClass, mf.runtimeClass.getSimpleName)

  def dtoMappingClassName = "dto.DtoMapping"
  def defaultApiRoleName  = "ADMIN"

  lazy val viewNameToClassMap: Map[String, Class[_ <: Dto]] = {
    val objectClass = Class.forName(dtoMappingClassName + "$")
    objectClass.getField("MODULE$").get(objectClass)
      .asInstanceOf[{ val viewNameToClass: Map[String, Class[_ <: Dto]] }].viewNameToClass
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

  protected def toAppViewDef(vd: ViewDef, isInline: Boolean): ViewDef = {
    import KnownAuthOps._
    import KnownViewExtras._
    import KnownFieldExtras._
    import ViewDefExtrasUtils._
    val viewDef = QuereaseMetadata.toQuereaseViewDef(vd)
    val appFields = viewDef.fields map { f =>
      import FieldDefExtrasUtils._

      val (label, comments) =
        Option(f.comments).map(splitToLabelAndComments).map{
          case (null, c) => (fieldLabelFromName(f), c)
          case lc => lc
        } getOrElse (fieldLabelFromName(f), null)
      val fieldName = f.fieldName

      val fieldApiKnownOps = Set(Readwrite, Readonly, NoInsert, NoUpdate, Excluded)
      val fieldApi = getStringSeq(FieldApi, f.extras) match {
        case api =>
          val ops = api.flatMap(_.trim.split(",").toList).map(_.trim).filter(_ != "").toSet
          val opt = fieldOptionsSelf(f)
          def isPk = viewDef.table != null &&
            tableMetadata.tableDefOption(viewDef).flatMap(_.pk).map(_.cols).contains(Seq(fieldName))
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

      val required = getBooleanExtra(viewDef, f, Required)

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
        Option(f.extras).map(_ -- handledExtras).orNull
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
          (apiToRoles, Set(x.toUpperCase), false)
        else
          (apiToRoles, roles + x.toUpperCase, false)
    }._1

    val limit = getIntExtra(Limit, viewDef) getOrElse 100
    val explicitDb = getBooleanExtra(ExplicitDb, viewDef)
    val decodeRequest = getBooleanExtraOpt(DecodeRequest, viewDef).forall(identity)
    val actions = Action().foldLeft(Map[String, Action]()) { (res, actionName) =>
      val opParser = new OpParser(viewDef.name, tresqlUri, opParserCache(viewDef.name))
      val a = parseAction(s"${viewDef.name}.$actionName", getSeq(actionName, viewDef.extras), opParser)
      if (a.steps.nonEmpty) res + (actionName -> a) else res
    }

    val extras =
      Option(viewDef.extras)
        .map(_ -- handledViewExtras)
        .map(_.filterNot(e => knownPrefixes.exists(e._1 startsWith _ + " "))) // auth can be used as prefix, too
        .orNull
    val unknownKeys =
      Option(extras)
        .map(_ -- (if (isInline) knownInlineViewExtras else knownViewExtras))
        .map(_.keySet.filterNot(_.toLowerCase startsWith "todo"))
        .filterNot(_.isEmpty).orNull
    if (unknownKeys != null)
      sys.error(
        s"Unknown properties for viewDef ${viewDef.name}: ${unknownKeys.mkString(", ")}")


    ViewDef(name, db, table, tableAlias, joins, filter,
      viewDef.groupBy, viewDef.having, orderBy, extends_,
      comments, appFields, viewDef.saveTo, extras)
      .updateWabaseExtras(_ => AppViewDef(limit, explicitDb, decodeRequest, auth, apiToRoles, actions, Map.empty))
  }

  protected def transformAppViewDefs(viewDefs: Map[String, ViewDef]): Map[String, ViewDef] =
    Option(viewDefs)
      .map(resolveViewDbAccessKeys(_, jobDefLoader.nameToJobDef))
      .orNull

  protected def transformJobDefs(jobDefs: Map[String, JobDef]) =
    Option(jobDefs)
      .map(resolveJobDbAccessKeys(nameToViewDef, _))
      .orNull

  /* Sets field options to horizontal auth statements if such are defined for child view, so that during ort auth
   * for child views are applied.
   */
  @deprecated("Results of this method are not used and this method will be removed", "6.0")
  protected def resolveAuth(viewDefs: Map[String, ViewDef]): Map[String, ViewDef] = viewDefs.map { case (name, viewDef) =>
    name -> viewDef.copy(fields = viewDef.fields.map { field =>
      viewDefs.get(field.type_.name)
        .filter(v => !field.isExpression && field.type_.isComplexType &&
          (v.auth.forInsert.nonEmpty || v.auth.forDelete.nonEmpty || v.auth.forUpdate.nonEmpty))
        .map { v =>
          def tresql(filter: Seq[String]) =
            if (filter.isEmpty) "null" else filter.map(a => s"($a)").mkString(" & ")
          field.copy(options = field.options + s"|${
            tresql(v.auth.forInsert)}, ${tresql(v.auth.forDelete)}, ${tresql(v.auth.forUpdate)}")
        }.getOrElse(field)
    })
  }

  private lazy val viewNameToQueryVariablesCompilerCache = {
    val cache = new ConcurrentHashMap[String, Seq[ast.Variable]]
    cache.putAll(viewNameToQueryVariablesCache.asJava)
    cache
  }

  protected def actionQueries(actionName: String, objName: String, action: Action): Set[(String,String)] = {
    case class QueriesState(dbStack: List[String], queries: scala.collection.mutable.Set[(String, String)])
    lazy val opTresqlTrav: OpTresqlTraverser[QueriesState] =
      opTresqlTraverser(opTresqlTrav, stepTresqlTrav)(st => {
        case Action.Db(action, _, dbk :: _) =>
          traverseAction(action)(stepTresqlTrav)(
            st.copy(value = st.value.copy(dbStack = dbk.db :: st.value.dbStack))
          )
          st
        case ViewCall(_, _, data) => opTresqlTrav(st)(data) // do not go to process view call since all views are compiled
        case _: Action.Job => st // do not go to process job call since all jobs are compiled in a loop
      })
    lazy val stepTresqlTrav: StepTresqlTraverser[QueriesState] =
      stepTresqlTraverser(opTresqlTrav)(st => {
        case Validations(_, validations, db) =>
          val v = viewDef(st.name)
          validationsQueryString(v, validations).flatMap { valStr =>
            db.flatMap(k => Option(k.db)).map("|" + _ + ":" + valStr)
              .orElse(Option(valStr))
              .map { tresql =>
                st.copy(value = st.tresqlExtractor(st.value)(Action.Tresql(tresql)))
              }
          }.getOrElse(st)
      })
    val state = State[QueriesState](
      actionName, objName, Map(), Map(),
      tresqlExtractor = cq => tresql => {
        val tresqlString = tresql.tresql
        cq.queries += (cq.dbStack.headOption.orNull -> tresqlString)
        cq
      },
      viewExtractor = v => _ => v,
      jobExtractor = v => _ => v,
      processed = Set(), value = QueriesState(Nil, scala.collection.mutable.Set[(String, String)]())
    )
    traverseAction(action)(stepTresqlTrav)(state).value.queries.toSet
  }
  override def allQueryStrings(viewDef: ViewDef): Seq[CompilationUnit] = {
    super.allQueryStrings(viewDef) ++ viewDef.actions.flatMap { case (actionName, action) =>
      val objName = viewDef.name
      actionQueries(actionName, objName, action)
        .map(compilationUnit("action-queries", s"$objName.$actionName", viewDef.db, _))
    }
  }

  override protected def generateQueriesForCompilation(log: => String => Unit): Seq[CompilationUnit] = {
    val viewQueries = super.generateQueriesForCompilation(log)
    log(s"Generating queries to be compiled for ${nameToJobDef.size} jobs")
    val startTime = System.currentTimeMillis
    val jobQueries = nameToJobDef.flatMap { case (jobName, job) =>
      actionQueries(JobAct, jobName, job.action).map(compilationUnit("action-queries", s"$jobName.$JobAct", job.db, _))
    }
    val endTime = System.currentTimeMillis
    log(s"Query generation done in ${endTime - startTime} ms, ${jobQueries.size} queries generated")
    viewQueries ++ jobQueries
  }

  private def compilationUnit(category: String, source: String, defaultDb: String, query: (String, String)) = {
    val (db, q) = query
    CompilationUnit(category, source, if (db == null) defaultDb else db, q)
  }

  override protected def compileQueries(
    category: String,
    compilationUnits: Seq[CompilationUnit],
    previouslyCompiledQueries: Set[String],
    showFailedViewQuery: Boolean,
    log: => String => Unit,
  ): Int = category match {
    case "queries" =>
      log(s"Compiling $category - ${compilationUnits.size} total")
      val startTime = System.currentTimeMillis
      val scalaMacros: Any = Option(tresqlMetadata.macrosClass).map(getObjectOrNewInstance(_, "metadata macros")).orNull
      val macroResources = new MacroResourcesImpl(scalaMacros, tresqlMetadata)
      val dbToCompiler = compilationUnits.map(_.db).toSet.map { (db: String) =>
       val compiler = new QueryParser(macroResources, new SimpleCache(parserCacheSize)) with org.tresql.compiling.Compiler {
        override val metadata = if (db == null) tresqlMetadata else tresqlMetadata.extraDbToMetadata(db)
        override val extraMetadata = tresqlMetadata.extraDbToMetadata
       }
       db -> compiler
      }.toMap
      val compiledQueries = collection.mutable.Set[String](previouslyCompiledQueries.toSeq: _*)
      var compiledCount = 0
      compilationUnits.foreach { case cu @ CompilationUnit(_, viewName, db, q) =>
        if (!compiledQueries.contains(cu.queryStringWithContext) ||
            viewNameToQueryVariablesCompilerCache.get(viewName) == null) {
          val compiler = dbToCompiler(db)
          try compiler.compile(compiler.parseExp(q)) catch { case util.control.NonFatal(ex) =>
            val msg = s"\nFailed to compile $viewName query: ${ex.getMessage}" +
              (if (showFailedViewQuery) s"\n$q" else "")
            throw new RuntimeException(msg, ex)
          }
          viewNameToQueryVariablesCompilerCache.put(viewName, compiler.extractVariables(q))
          if (!compiledQueries.contains(cu.queryStringWithContext)) {
            compiledCount += 1
            compiledQueries += cu.queryStringWithContext
          }
        }
      }
      val endTime = System.currentTimeMillis
      val allQueries = compilationUnits.map(_.queryStringWithContext).toSet
      log(
        s"Query compilation done - ${endTime - startTime} ms, " +
        s"queries compiled: $compiledCount" +
        (if (compiledCount != allQueries.size) s" of ${allQueries.size}" else ""))
      compiledCount
    case _ => super.compileQueries(
      category, compilationUnits, previouslyCompiledQueries, showFailedViewQuery, log)
  }

  /** Clear all caches used for query compilation */
  override protected def clearAllCaches(): Unit = {
    super.clearAllCaches()
    viewNameToQueryVariablesCompilerCache.clear()
    actionOpCache.clear()
  }

  override protected def serializedCaches: Map[String, Array[Byte]] = {
    import io.bullet.borer._
    val joinData: Map[String, Map[String, Exp]] =
      joinsParser.asInstanceOf[TresqlJoinsParser].dbToCompilerAndCache.map {
        case (db, (_, c)) => Option(db).getOrElse("null") -> c.map(_.toMap).getOrElse(Map[String, Exp]())
      }

    import CacheIo.varCodec
    val serializedVars = Map(
      ViewNameToQueryVariablesCacheName -> Cbor.encode(viewNameToQueryVariablesCompilerCache.asScala.toMap).toByteArray)

    import CacheIo.expCodec
    val serializedJoins = Map(JoinsCompilerCacheName -> Cbor.encode(joinData).toByteArray)

    val serializedQeParserCache = parser.cache.map { cache =>
      Map(AppQuereaseParserCacheName -> Cbor.encode(cache.toMap).toByteArray)
    }.getOrElse(Map.empty)

    super.serializedCaches ++
      serializedQeParserCache ++
      OpParser.serializeOpCaches(actionOpCache) ++
      serializedJoins ++
      serializedVars
  }

  protected def createJoinsParserCache(db: String): Option[Cache] =
    joinsParserCacheFactory(joinsParserCache, parserCacheSize)(db)

  private def resolveDbAccessKeys(
    action: String, name: String,
    viewDefs: Map[String, ViewDef], jobDefs: Map[String, JobDef],
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

    val state = State[Seq[DbAccessKey]](action, name, viewDefs, jobDefs,
      tresqlExtractor = dbKeys => tresql => dbKeys ++ tresql.dbs.map(DbAccessKey),
      viewExtractor = dbkeys => vd =>
        dbkeys ++ (if (vd.db != null) Seq(DbAccessKey(vd.db)) else Nil),
      jobExtractor = dbkeys => jd =>
        dbkeys ++ Option(jd.db).map(db => Seq(DbAccessKey(db))).getOrElse(Nil),
      processed = Set(), value = Nil
    )
    fun(stepTresqlTrav)(state)
  }

  protected def resolveViewDbAccessKeys(
    viewDefs: Map[String, ViewDef],
    jobDefs: Map[String, JobDef],
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
        val st = resolveDbAccessKeys(action_, viewName, viewDefs, jobDefs, processView[Seq[DbAccessKey]])
        val dbkeys = st.value.distinct
        (action, dbkeys)
      }.toMap
      viewDef.updateWabaseExtras(_.copy(actionToDbAccessKeys = actionToDbAccessKeys))
    }
  }

  protected def resolveJobDbAccessKeys(
    viewDefs: Map[String, ViewDef],
    jobDefs: Map[String, JobDef],
  ): Map[String, JobDef] = {
    jobDefs.transform { case (jobName, jobDef) =>
      import Action._
      import TresqlExtraction._
      val st = resolveDbAccessKeys(JobAct, jobName, viewDefs, jobDefs, processJob[Seq[DbAccessKey]])
      val dbkeys = st.value.distinct
      jobDef.copy(dbAccessKeys = dbkeys)
    }
  }

  protected def parseAction(objectName: String, stepData: Seq[Any], opParser: OpParser): Action = {
    val namedStepRegex = """(?U)(?:((?:\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)(?:\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)*)\s*=\s*)?(.+)""".r
    // matches - 'validations validation_name [db:cp]'
    val validationRegex = new Regex(s"(?U)${Action.ValidationsKey}(?:\\s+(\\w+))?(?:\\s+\\[(?:\\s*(\\w+)?\\s*(?::\\s*(\\w+)\\s*)?)\\])?")
    val arr_regex = "(?:\\s+\\[([^\\[^\\]]+)\\])?"
    val db_use_or_transaction_regex = new Regex(s"(${Action.DbUseKey}|${Action.TransactionKey})$arr_regex")
    val removeVarStepRegex = {
      val ident = """\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*"""
      val str_lit = """"[^"]*+"|'[^']*+'"""
      s"""(?U)(?:(?:($ident)|($str_lit))\\s*-=\\s*)""".r
    }
    val setEnvRegex = """setenv\s+(.+)""".r //dot matches new line as well
    val returnRegex = """return\s+(.+)""".r //dot matches new line as well
    val redirectToKeyOpRegex = """redirect\s+([_\p{IsLatin}][_\p{IsLatin}0-9]*)""".r
    val redirectOpRegex = """redirect\s+(.+)""".r
    val statusOpRegex = """status(?:\s+(\w+))?(?:\s+(.+))?""".r
    val commitOpRegex = """commit""".r
    val ifOpRegex = """if\s+(.+)""".r
    val elseOpRegex = """else""".r
    val foreachOpRegex = """foreach\s+(.+)""".r
    import ViewDefExtrasUtils._
    val steps = stepData.map { step =>
      def parseOp(st: String): Action.Op = {
        def statusParameterIdx(exp: String) = if (exp == null) -1 else {
          tresqlUri.queryStringColIdx(parser.parseExp(exp))(parser)
        }
        if (redirectToKeyOpRegex.pattern.matcher(st).matches()) {
          val redirectToKeyOpRegex(name) = st
          Action.RedirectToKey(name)
        } else if (redirectOpRegex.pattern.matcher(st).matches()) {
          val redirectOpRegex(tresql) = st
          Action.Status(Option(303), tresql, statusParameterIdx(tresql))
        } else if (statusOpRegex.pattern.matcher(st).matches()) {
          val statusOpRegex(status, bodyTresql) = st
          val code = Option(status).collect {
            case "ok" => 200
            case x => throw new IllegalArgumentException(s"Status must be 'ok' or omitted, instead '$x' encountered.")
          }
          require(code.nonEmpty || bodyTresql != null, s"Empty status operation!")
          Action.Status(code, bodyTresql, statusParameterIdx(bodyTresql))
        } else if (commitOpRegex.pattern.matcher(st).matches()) {
          Action.Commit
        } else {
          opParser.parseOperation(st)
        }
      }
      def parseStringStep(name: Option[String], statement: String): Action.Step = {
        def parseSt(st: String, varTrs: List[VariableTransform]) = {
          def setEnvOrRetStep(createStep: Action.Op => Action.Step, stepRegex: Regex): Action.Step = {
            val stepRegex(opStr) = st
            val op =
              if (opStr.contains("="))
                Try {
                  val p = parser.asInstanceOf[AppQuereaseDefaultParser] // FIXME get rid of typecast
                  p.parseWithParser(p.varsTransforms)(opStr)
                }.toOption.getOrElse(parseOp(opStr))
              else parseOp(opStr)
            createStep(op)
          }

          if (setEnvRegex.pattern.matcher(st).matches()) {
            setEnvOrRetStep(Action.SetEnv(name, varTrs, _), setEnvRegex)
          } else if (returnRegex.pattern.matcher(st).matches) {
            setEnvOrRetStep(Action.Return(name, varTrs, _), returnRegex)
          } else {
            Action.Evaluation(name, varTrs, parseOp(st))
          }
        }

        if (statement.contains("->")) {
          val p = parser.asInstanceOf[AppQuereaseDefaultParser] // FIXME get rid of typecast
          Try(p.parseWithParser(p.stepWithVarsTransform)(statement))
            .map { case (vtrs, st) => parseSt(st, vtrs) }
            .toOption
            .getOrElse(parseSt(statement, Nil))
        } else {
          parseSt(statement, Nil)
        }
      }
      def parseStep(anyStep: Any): Action.Step = {
        anyStep match {
          case s: String if removeVarStepRegex.pattern.matcher(s).matches() =>
            val removeVarStepRegex(ident, str_lit) = s
            val name = if (ident != null) ident else str_lit.substring(1, str_lit.length - 1)
            Action.RemoveVar(Some(name))
          case s: String =>
            val namedStepRegex(name, st) = s
            parseStringStep(Option(name), st)
          case jm: java.util.Map[String, Any]@unchecked if jm.size() == 1 =>
            val m = jm.asScala.toMap
            val (name, value) = m.head
            if (validationRegex.pattern.matcher(name).matches()) {
              val validationRegex(vn, db, cp) = name
              val validations = getSeq(name, m).map(_.toString)
              Action.Validations(
                Option(vn),
                validations,
                if (db == null) None else Option(DbAccessKey(db))
              )
            } else {
              value match {
                case jm: java.util.Map[String@unchecked, _] =>
                  // may be 'if', 'foreach', 'db ...' step
                  parseStep(jm) match {
                    case e: Action.Evaluation => e.copy(name = Option(name))
                    case x => sys.error(s"Invalid step '$name' value here: ($x), expected Evaluation step.")
                  }
                case al: java.util.ArrayList[_] if name != null =>
                  // 'if', 'foreach', 'db ...' step
                  val namedStepRegex(varName, opStr) = name
                  def pa = parseAction(objectName, al.asScala.toList, opParser)
                  val op =
                    if (ifOpRegex.pattern.matcher(opStr).matches()) {
                      val ifOpRegex(condOpSt) = opStr
                      Action.If(parseOp(condOpSt), pa)
                    } else if (elseOpRegex.pattern.matcher(opStr).matches()) {
                      Action.Else(pa)
                    } else if (foreachOpRegex.pattern.matcher(opStr).matches()) {
                      val foreachOpRegex(initOpSt) = opStr
                      Action.Foreach(parseOp(initOpSt), pa)
                    } else if (db_use_or_transaction_regex.pattern.matcher(opStr).matches()) {
                      val db_use_or_transaction_regex(action, dbs) = opStr
                      val db_keys = if (dbs == null) Nil else dbs.split(",").toList
                      Action.Db(pa, action == Action.DbUseKey, db_keys.map(DbAccessKey))
                    } else Action.Block(pa)
                  val eval_var_name = op match {
                    case _: Action.Block => name
                    case _ => varName
                  }
                  Action.Evaluation(Option(eval_var_name), Nil, op)
                case x => parseStringStep(Option(name), x.toString)
              }
            }
          case x =>
            sys.error(s"'$objectName' parsing error, invalid value: $x")
        }
      }
      parseStep(step)
    }.toList
    //coalesce else op into if
    val coalescedSteps = if (steps.isEmpty) Nil else
      (steps.tail.foldLeft(steps.head -> List[Action.Step]()) { case ((p, r), s) =>
        s match {
          case Action.Evaluation(_, _, elseOp: Action.Else) => p match {
            case ifEv@Action.Evaluation(_, _, ifOp: Action.If) =>
              (null, ifEv.copy(op = ifOp.copy(elseAct = elseOp.action)) :: r)
            case _ => sys.error(s"else statement must follow if statement, instead found '$p'")
          }
          case _ => (s, if (p != null) p :: r else r)
        }
      } match {
        case (null, r) => r
        case (x, r) => x :: r
      }).reverse
    Action(coalescedSteps)
  }

  abstract class AppQuereaseDefaultParser(cache: Option[Cache]) extends DefaultParser(cache) {
    private def varsTransform: MemParser[VariableTransform] = {
      def v2s(v: Variable) = (v.variable :: v.members) mkString "."
      (variable | ("(" ~> ident ~ "=" ~ variable <~ ")")) ^^ {
        case v: Variable => VariableTransform(v2s(v), None)
        case (v1: String) ~ _ ~ (v2: Variable) => VariableTransform(v2s(v2), Option(v1))
      }
    }
    def varsTransforms: MemParser[VariableTransforms] = {
      rep1sep(varsTransform, "+") ^^ (VariableTransforms) named "var-transforms"
    }
    def stepWithVarsTransform: MemParser[(List[VariableTransform], String)] = {
      (varsTransforms ~ ("->" ~> "(?s).*".r)) ^^ {
        case VariableTransforms(vts) ~ step => vts -> step
      } named "step-with-vars-transform"
    }
  }

  object AppQuereaseDefaultParser extends AppQuereaseDefaultParser(createParserCache)

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
        val cache = Cbor.decode(res).to[Map[String, Exp]].value
        logger.debug(s"App querease parser cache loaded for ${cache.size} expressions")
        cache
      }
    }
    val cache = new SimpleCache(parserCacheSize)
    cache.load(loadAppQuereaseParserCache(resourceLoader))
    Some(cache)
  }
  override val parser: QuereaseExpressions.Parser = this.AppQuereaseDefaultParser
}

class OpParser(viewName: String, tresqlUri: TresqlUri, cache: OpParser.Cache)
  extends QueryParsers { self =>
  import AppMetadata.Action._
  import AppMetadata.Action

  /** View action must be end with whitespace regexp so that no match is if space(s) is omitted between action and
    * view name since spaces are eliminated at the beginning of input before applying parser */
  val ActionRegex = new Regex(Action().mkString("(?U)(", "|", """)\s+"""))
  val InvocationRegex = """(?U)\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*(\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)+""".r
  val ViewNameRegex = "(?U)\\w+".r
  val ConfPropRegex = """\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*(?:\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*+)*""".r

  def parseOperation(op: String): Op = cache.get(op).getOrElse {
    val parsedOp = phrase(operation)(new scala.util.parsing.input.CharSequenceReader(op)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }
    cache.put(op, parsedOp)
    parsedOp
  }

  def tresqlOp: MemParser[Tresql] = opt(opResultType) ~ expr ^^ { case rt ~ e =>
    val te = transformer {
      case f@ast.Fun("build_cursors",
        (o@ast.Obj(ast.Ident("this" :: Nil), _, _, _, _)) :: tail, false, None, None
      ) => f.copy(parameters = o.copy(obj = ast.Ident(viewName :: Nil)) :: tail)
    }(e)
    val dbs = traverser(dbExtractor)(Nil)(e)
    Tresql(te.tresql, dbs, rt)
  } named "tresql-op"
  def viewOp: MemParser[ViewCall] = ActionRegex ~ ViewNameRegex ~ opt(operation) ^^ {
    case action ~ view ~ op =>
      ViewCall(action.trim /* trim ending whitespace */, view, op.orNull)
  }  named "view-op"
  def uniqueOp: MemParser[Unique] = opt(opResultType) ~ (("unique_opt" | "unique") ~ operation) ^^ {
    case rt ~ (mode ~ op) => Unique(op, mode == "unique_opt", rt)
  } named "unique-op"

  def invocationOp: MemParser[Invocation] = opt(opResultType) ~ InvocationRegex ~ opt(operation) ^^ {
    case rt ~ res ~ arg =>
      val idx = res.lastIndexOf('.')
      Action.Invocation(res.substring(0, idx), res.substring(idx + 1), arg.orNull, rt)
  } named "invocation-op"
  def resourceOp: MemParser[Resource] = "resource" ~> tresqlOp ~ opt(tresqlOp) ^^ {
    case nameTresql ~ ctTresql => Resource(nameTresql, ctTresql.orNull)
  } named "resource-op"
  def fileOp: MemParser[File] = opt(opResultType) ~ ("file" ~> tresqlOp) ^^ {
    case conformTo ~ e => File(e, conformTo)
  } named "file-op"
  def toFileOp: MemParser[ToFile] = "to file" ~> operation ~ opt(tresqlOp) ~ opt(tresqlOp) ^^ {
    case op ~ fileName ~ contentType =>
      ToFile(op, fileName.orNull, contentType.orNull)
  } named "to-file-op"
  def templateOp: MemParser[Template] = {
    val Data = "data"
    val Filename = "filename"
    val args = Set(Data, Filename)
    def findArg(name: String, idx: Int, l: List[(String, Op)]) =
      l.find(_._1 == name).orElse(l.lift(idx).filter(_._1 == null)).map(_._2)
    "template" ~> tresqlOp ~ namedOps(args) ^^ {
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
  def emailOp: MemParser[Email] = "email" ~> tresqlOp ~ operation ~ operation ~ rep(operation) ^^ {
    case data ~ subj ~ body ~ att => Email(data, subj, body, att)
  } named "email-op"
  def httpOp: MemParser[Http] = {
    def tu(uri: Exp) = tresqlUri.parse(uri)(self)
    def http_get_delete: MemParser[Http] =
      "http" ~> opt("get" | "delete") ~ bracesTresql ~ opt(tresqlOp) ^^ {
        case method ~ uri ~ headers =>
          Http(method.getOrElse("get"), tu(uri), headers.orNull, null)
      } named "http-get-delete-op"
    def http_post_put: MemParser[Http] =
      "http" ~> ("post" | "put") ~ bracesTresql ~ operation ~ opt(tresqlOp) ^^ {
        case method ~ uri ~ op ~ headers =>
          Http(method, tu(uri), headers.orNull, op)
      } named "http-post-put-op"
    opt(opResultType) ~ (http_post_put | http_get_delete) ^^ {
      case conformTo ~ http => http.copy(conformTo = conformTo)
    } named "http-op"
  }
  def jsonCodecOp: MemParser[JsonCodec] = """(from|to)""".r ~ "json" ~ operation ^^ {
    case mode ~ _ ~ op => JsonCodec(mode == "to", op)
  } named "json-op"
  def jobOp: MemParser[Job] = JobAct ~> expr ^^ {
    case ast.StringConst(value) => Job(value, false)
    case ast.Obj(i: ast.Ident, _, _, _, _) => Job(i.tresql, false)
    case e => Job(e.tresql, true)
  } named "job-op"
  def confOp: MemParser[Conf] = {
    val parType = new Regex(ConfTypes.types.map(_.name).mkString("|"))
    "conf" ~> opt(parType) ~ ConfPropRegex ^^ {
      case pt ~ param => Conf(param, ConfTypes.parse(pt.orNull))
    }
  } named "conf-op"
  def httpHeaderOp: MemParser[HttpHeader] = "extract header" ~> ".*".r ^^ {
    case h => HttpHeader(h)
  } named "http-header-op"

  def bracesOp: MemParser[Op] = "(" ~> operation <~ ")" named "braces-op"
  def bracesTresql: MemParser[Exp] = (("(" ~> expr <~ ")") | expr) named "braces-tresql-op"
  def namedOps(allowedNames: Set[String]): MemParser[List[(String, Op)]] = {
    val namedOp: MemParser[(String, Op)] =
      opt(ident <~ "=") ~ operation ^? ( {
        case Some(name) ~ op if allowedNames(name) => (name, op)
        case _ ~ op => (null, op)
      }, {
        case n ~ _ => s"Illegal argument name - $n, allowed arguments - $allowedNames"
      }) named "named-op"
    rep(namedOp)
  } named "named-ops"
  def operation: MemParser[Op] = (viewOp | jobOp | confOp | uniqueOp | invocationOp |
    httpOp | resourceOp | fileOp | toFileOp | templateOp | emailOp |
    jsonCodecOp | httpHeaderOp | bracesOp | tresqlOp) named "operation"

  private def opResultType: MemParser[OpResultType] = {
    sealed trait ResType
    case object NoType extends ResType
    case class ViewType(vn: String) extends ResType
    def noType: Parser[ResType] = "any" ^^^ NoType
    def viewType: Parser[ResType] = opt("`") ~> ViewNameRegex <~ opt("`") ^^ ViewType

    "as" ~> (noType | ((opt("`") ~> viewType <~ opt("`")) ~ opt("*"))) ^^ {
      case NoType => OpResultType(null)
      case ViewType(typ) ~ (coll: Option[String]@unchecked) => OpResultType(typ, coll.nonEmpty)
      case x => sys.error(s"Knipis, unexpected op result type: $x")
    } named "op-result-type"
  }
}

object OpParser extends Loggable {
  class Cache(maxSize: Int) extends SimpleCacheBase[Action.Op](maxSize, "OpParser cache")

  val QuereaseActionOpCacheName = "querease-action-op-cache.cbor"
  def loadSerializedOpCaches(getResourceAsStream: String => InputStream): Map[String, Map[String, Action.Op]] = {
    val res = getResourceAsStream(s"/$QuereaseActionOpCacheName")
    if (res == null) {
      logger.debug(s"No querease view action op cache resource - '/$QuereaseActionOpCacheName' found")
      Map()
    } else {
      import io.bullet.borer._
      import CacheIo.opCodec
      val cache =
        Cbor.decode(res).to[Map[String, Map[String, Action.Op]]].value
      logger.debug(s"Querease action op cache loaded for ${cache.size} views.")
      cache
    }
  }

  def serializeOpCaches(caches: scala.collection.mutable.Map[String, Cache]): Map[String, Array[Byte]] = {
    import io.bullet.borer._
    import CacheIo.opCodec
    val actionOpData = caches.map { case (n, c) => (n, c.toMap) }.toMap
    Map(QuereaseActionOpCacheName -> Cbor.encode(actionOpData).toByteArray)
  }

  def createOpParserCache(initData: Map[String, Action.Op], maxSize: Int): Cache = {
    val c = new Cache(maxSize)
    c.load(initData)
    c
  }
}
object AppMetadata extends Loggable {

  case class AuthFilters(
    forGet: Seq[String],
    forList: Seq[String],
    forInsert: Seq[String],
    forUpdate: Seq[String],
    forDelete: Seq[String]
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
  def loadViewNameToQueryVariablesCache(getResourceAsStream: String => InputStream): Map[String, Seq[ast.Variable]] = {
    val res = getResourceAsStream(s"/$ViewNameToQueryVariablesCacheName")
    if (res == null) {
      logger.debug(s"Query variables cache resource not found: '/$ViewNameToQueryVariablesCacheName'")
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

  val JobAct = "job"

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
    def apply() =
      Set(Get, List, Save, Insert, Update, Upsert, Delete, Create, Count)

    val ValidationsKey = "validations"
    val DbUseKey = "db use"
    val TransactionKey = "transaction"
    val OffsetKey = "offset"
    val LimitKey  = "limit"
    val OrderKey  = "sort"

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


    sealed trait Op
    sealed trait CastableOp extends Op {
      def conformTo: Option[OpResultType]
    }
    sealed trait Step {
      def name: Option[String]
    }

    case class VariableTransform(from: String, to: Option[String] = None)
    case class OpResultType(viewName: String = null, isCollection: Boolean = false)

    case class Tresql(tresql: String,
                      dbs: List[String] = Nil,
                      conformTo: Option[OpResultType] = None) extends CastableOp
    case class ViewCall(method: String, view: String, data: Op = null) extends Op
    case class RedirectToKey(name: String) extends Op
    case class Unique(innerOp: Op, opt: Boolean, conformTo: Option[OpResultType] = None) extends CastableOp
    case class Invocation(className: String,
                          function: String,
                          arg: Op = null,
                          conformTo: Option[OpResultType] = None) extends CastableOp
    case class Status(code: Option[Int], bodyTresql: String = null, parameterIndex: Int = -1) extends Op
    case class VariableTransforms(transforms: List[VariableTransform]) extends Op
    case class Foreach(initOp: Op, action: Action) extends Op
    case class If(cond: Op, action: Action, elseAct: Action = null) extends Op
    case class Resource(nameTresql: Tresql, contentTypeTresql: Tresql = null) extends Op
    case class File(idShaTresql: Tresql, conformTo: Option[OpResultType] = None) extends CastableOp
    case class ToFile(contentOp: Op, nameTresql: Tresql = null, contentTypeTresql: Tresql = null) extends Op
    case class Template(templateTresql: Tresql, dataOp: Op = null, filenameTresql: Tresql = null) extends Op
    case class Email(emailTresql: Tresql, subject: Op, body: Op, attachmentsOp: List[Op] = Nil) extends Op
    case class Http(method: String,
                    uriTresql: TresqlUri.TrUri,
                    headerTresql: Tresql = null,
                    body: Op = null,
                    conformTo: Option[OpResultType] = None) extends CastableOp
    case class HttpHeader(name: String) extends Op
    case class Db(action: Action, doRollback: Boolean, dbs: List[DbAccessKey]) extends Op
    case class Conf(param: String, paramType: ConfType = null) extends Op
    case class JsonCodec(encode: Boolean, op: Op) extends Op
    /** This operation exists only in parsing stage for if operation */
    case class Else(action: Action) extends Op
    case class Block(action: Action) extends Op
    /** if isDynamic is false, nameTresql parameter is expected to be indentifier or string constant
     * (not to be evaluated as tresql to get job name). */
    case class Job(nameTresql: String, isDynamic: Boolean) extends Op
    case object Commit extends Op

    case class Evaluation(name: Option[String], varTrans: List[VariableTransform], op: Op) extends Step
    case class SetEnv(name: Option[String], varTrans: List[VariableTransform], value: Op) extends Step
    case class Return(name: Option[String], varTrans: List[VariableTransform], value: Op) extends Step
    case class Validations(name: Option[String], validations: Seq[String], db: Option[DbAccessKey]) extends Step
    case class RemoveVar(name: Option[String]) extends Step

    type OpTraverser[T] = T => PartialFunction[Op, T]
    type StepTraverser[T] = T => PartialFunction[Step, T]

    def opTraverser[T](opTrav: => OpTraverser[T], stepTrav: => StepTraverser[T])(
        extractor: OpTraverser[T]): OpTraverser[T] = {
      def traverse(state: T): PartialFunction[Op, T] = {
        case _: Tresql | _: RedirectToKey | _: Status |
             _: VariableTransforms | _: File | _: Conf | _: HttpHeader | _: Job |
             _: Resource | Commit | null => state
        case o: ViewCall => opTrav(state)(o.data)
        case Unique(o, _, _) => opTrav(state)(o)
        case Foreach(o, a) => traverseAction(a)(stepTrav)(opTrav(state)(o))
        case If(o, a, e) =>
          val r = traverseAction(a)(stepTrav)(opTrav(state)(o))
          if (e == null) r else traverseAction(e)(stepTrav)(r)
        case o: ToFile => opTrav(state)(o.contentOp)
        case o: Template => opTrav(state)(o.dataOp)
        case Email(_, s, b, a) => a.foldLeft(opTrav(opTrav(state)(s))(b))(opTrav(_)(_))
        case o: Http => opTrav(state)(o.body)
        case Db(a, _, _) => traverseAction(a)(stepTrav)(state)
        case Block(a) => traverseAction(a)(stepTrav)(state)
        case JsonCodec(_, o) => opTrav(state)(o)
        case i: Invocation => opTrav(state)(i.arg)
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
      action.steps.foldLeft(_) { stepTraverser(_)(_) }

    object TresqlExtraction {
      type ViewExtractor[T] = T => ViewDef => T
      type JobExtractor[T] = T => JobDef => T
      type TresqlExtractor[T] = T => Tresql => T
      type StepTresqlTraverser[T] = StepTraverser[State[T]]
      type OpTresqlTraverser[T] = OpTraverser[State[T]]

      case class State[T](
        action: String, name: String,
        viewDefs: Map[String, ViewDef], jobDefs: Map[String, JobDef],
        tresqlExtractor: TresqlExtractor[T],
        viewExtractor: ViewExtractor[T], jobExtractor: JobExtractor[T],
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
                .foldLeft(newVal -> Set(vn)) {
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

      def processJob[T](stepTresqlTrav: => StepTresqlTraverser[T])(s: State[T]): State[T] = {
        val jd = s.jobDefs(s.name)
        val newVal = s.jobExtractor(s.value)(jd)
        val s1 = s.copy(value = newVal, processed = s.processed + (s.action -> s.name))
        traverseAction(jd.action)(stepTresqlTrav)(s1)
      }

      def opTresqlTraverser[T](opTresqlTrav: => OpTresqlTraverser[T],
        stepTresqlTrav: => StepTresqlTraverser[T])(extractor: OpTresqlTraverser[T]):
          OpTresqlTraverser[T] = {
        def traverse(state: State[T]): PartialFunction[Op, State[T]]= {
          val nv: T => Tresql => T = v => t => Option(t).map(state.tresqlExtractor(v)(_)).getOrElse(v)
          def opTrTr = opTresqlTrav(state)
          def us(s: State[T], v: T) = {
            s.copy(value = v)
          }
          {
            case t: Tresql => us(state, nv(state.value)(t))
            case Status(_, bodyTresql, _) =>
              if (bodyTresql == null) state else us(state, nv(state.value)(Tresql(bodyTresql)))
            case Resource(nameTresql, contentTypeTresql) =>
              val s1 = us(state, nv(state.value)(nameTresql))
              us(s1, nv(s1.value)(contentTypeTresql))
            case File(idShaTresql, _) => us(state, nv(state.value)(idShaTresql))
            case ToFile(contentOp, nameTresql, contentTypeTresql) =>
              val s1 = opTrTr(contentOp)
              val s2 = us(s1, nv(s1.value)(nameTresql))
              us(s2, nv(s2.value)(contentTypeTresql))
            case Template(templateTresql, dataOp, filenameTresql) =>
              val s = opTresqlTrav(us(state, nv(state.value)(templateTresql)))(dataOp)
              us(s, nv(s.value)(filenameTresql))
            case Email(emailTresql, s, b, a) =>
              a.foldLeft(
                opTresqlTrav(
                  opTresqlTrav(us(state, nv(state.value)(emailTresql))
                )(s))(b)
              )(opTresqlTrav(_)(_))
            case Http(_, uriTresql, headerTresql, body, _) =>
              def tresqlUriTresql(trUri: TresqlUri.TrUri): Tresql = trUri match {
                case p: TresqlUri.PrimitiveTresql => Tresql(p.origin)
                case t: TresqlUri.Tresql => Tresql(t.uriTresql)
              }
              val s1 = us(state, nv(state.value)(tresqlUriTresql(uriTresql)))
              val s2 = us(s1, nv(s1.value)(headerTresql))
              opTresqlTrav(s2)(body)
            case ViewCall(method, view, data) =>
              val vn = if (view == "this") state.name else view
              val ns = opTrTr(data)
              processView(stepTresqlTrav)(ns.copy(action = method, name = vn))
            case Job(nameTresql, isDynamic) =>
              if (isDynamic) us(state, nv(state.value)(Tresql(nameTresql)))
              else processJob(stepTresqlTrav)(state.copy(action = JobAct, name = nameTresql))
            case Invocation(_, _, o, _) => opTrTr(o)
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

  case class Action(steps: List[Action.Step])

  /** Database name (as used in mojoz metadata) and corresponding connection pool name */
  case class DbAccessKey(
    db: String,
  )

  trait AppViewDefExtras {
    val limit: Int
    val explicitDb: Boolean
    val decodeRequest: Boolean
    val auth: AuthFilters
    val apiMethodToRoles: Map[String, Set[String]]
    val actions: Map[String, Action]
    val actionToDbAccessKeys: Map[String, Seq[DbAccessKey]]
  }

  private [wabase] case class AppViewDef(
    limit: Int = 1000,
    explicitDb: Boolean = false,
    decodeRequest: Boolean = false,
    auth: AuthFilters = AuthFilters(Nil, Nil, Nil, Nil, Nil),
    apiMethodToRoles: Map[String, Set[String]] = Map(),
    actions: Map[String, Action] = Map(),
    actionToDbAccessKeys: Map[String, Seq[DbAccessKey]] = Map.empty,
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
    override val explicitDb = appExtras.explicitDb
    override val decodeRequest = appExtras.decodeRequest
    override val auth = appExtras.auth
    override val apiMethodToRoles = appExtras.apiMethodToRoles
    override val actions = appExtras.actions
    override val actionToDbAccessKeys = appExtras.actionToDbAccessKeys
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

  case class JobDef(
    name: String,
    action: Action,
    db: String = null,
    explicitDb: Boolean = false,
    dbAccessKeys: Seq[DbAccessKey] = Nil,
  )

  case class RouteDef(
    name: String,
    path: Regex,
    auth: Action.Invocation = null,
    state: Action.Invocation = null,
    requestFilter: Action.Invocation = null,
    responseTransformer: Action.Invocation = null,
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

  def this() = this(getClass.getResourceAsStream _)()

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
    val Validations = "validations"
    val ExplicitDb = "explicit db"
    val DecodeRequest = "decode request"
    val QuereaseViewExtrasKey = QuereaseMetadata.QuereaseViewExtrasKey
    val WabaseViewExtrasKey = AppMetadata.WabaseViewExtrasKey
    def apply() =
      Set(Api, Auth, Key, Limit, Validations, ExplicitDb, DecodeRequest, QuereaseViewExtrasKey, WabaseViewExtrasKey) ++
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
    val QuereaseFieldExtrasKey = QuereaseMetadata.QuereaseFieldExtrasKey
    val WabaseFieldExtrasKey = AppMetadata.WabaseFieldExtrasKey
    def apply() = Set(
      Domain, Hidden, Sortable, Visible, Required,
      FieldApi, Initial, QuereaseFieldExtrasKey, WabaseFieldExtrasKey)
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
}
