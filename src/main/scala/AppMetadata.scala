package org.wabase

import org.mojoz.metadata.{ViewDef => MojozViewDef}
import org.mojoz.metadata.{FieldDef => MojozFieldDef}
import org.mojoz.metadata.in._
import org.mojoz.metadata.io.MdConventions
import org.mojoz.metadata.out.SqlGenerator.SimpleConstraintNamingRules
import org.mojoz.querease._
import org.tresql.QueryParser
import org.tresql.parsing.{Col, Cols, Const}
import org.wabase.AppMetadata.Action.VariableTransform

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls
import scala.util.Try
import scala.util.matching.Regex

trait AppMetadata extends QuereaseMetadata { this: AppQuerease =>

  import AppMetadata._

  override lazy val metadataConventions: AppMdConventions = new DefaultAppMdConventions
  override lazy val nameToViewDef: Map[String, ViewDef] = {
    val mojozViewDefs =
      YamlViewDefLoader(tableMetadata, yamlMetadata, tresqlJoinsParser, metadataConventions, Seq("api"))
        .nameToViewDef
    toAppViewDefs(mojozViewDefs)
  }
  def toAppViewDefs(mojozViewDefs: Map[String, ViewDef]) = transformAppViewDefs {
    val inlineViewDefNames =
      mojozViewDefs.values.flatMap { viewDef =>
        viewDef.fields.filter { field =>
          field.type_.isComplexType &&
          field.type_.name == viewDef.name + "_" + field.name // XXX
        }.map(_.type_.name)
      }.toSet
    mojozViewDefs.map{ case (k, v) => (k, toAppViewDef(v, isInline = inlineViewDefNames.contains(v.name))) }.toMap
  }
  override def viewName[T <: AnyRef](implicit mf: Manifest[T]): String =
    classToViewNameMap.getOrElse(mf.runtimeClass, mf.runtimeClass.getSimpleName)

  def dtoMappingClassName = "dto.DtoMapping"
  def defaultApiRoleName  = "ADMIN"

  lazy val viewNameToClassMap: Map[String, Class[_ <: DTO]] = {
    val objectClass = Class.forName(dtoMappingClassName + "$")
    objectClass.getField("MODULE$").get(objectClass)
      .asInstanceOf[{ val viewNameToClass: Map[String, Class[_ <: DTO]] }].viewNameToClass
  }

  lazy val classToViewNameMap: Map[Class[_], String] = viewNameToClassMap.map(_.swap)

  lazy val viewNameToApiKeyFields: Map[String, Seq[FieldDef]] =
    viewNameToKeyFields.map { case (name, fields) => (name, fields.filterNot(_.api.excluded)) }
  lazy val viewNameToApiKeyColNames: Map[String, Seq[String]] =
    viewNameToApiKeyFields.map { case (name, fields) => (name, fields.map(_.name)) }
  lazy val viewNameToApiKeyFieldNames: Map[String, Seq[String]] =
    viewNameToApiKeyFields.map { case (name, fields) => (name, fields.map(_.fieldName)) }

  val knownApiMethods = Set("create", "count", "get", "list", "insert", "update", "save", "delete")
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
  val knownAuthOps = KnownAuthOps()

  object KnownViewExtras {
    val Api = "api"
    val Auth = "auth"
    val Key   = "key"
    val Limit = "limit"
    val Validations = "validations"
    val ConnectionPool = "cp"
    val QuereaseViewExtrasKey = QuereaseMetadata.QuereaseViewExtrasKey
    val WabaseViewExtrasKey = AppMetadata.WabaseViewExtrasKey
    def apply() =
      Set(Api, Auth, Key, Limit, Validations, ConnectionPool, QuereaseViewExtrasKey, WabaseViewExtrasKey) ++
        Action()
  }

  lazy val knownViewExtras = KnownViewExtras()
  protected val handledViewExtras = KnownViewExtras() - KnownViewExtras.QuereaseViewExtrasKey

  lazy val knownPrefixes = Set(KnownViewExtras.Auth)

  object KnownFieldExtras {
    val FieldApi = "field api" // avoid name clash with "api"
    val FieldDb = "field db"   // TODO remove - handled by options since querease 6.1

    val Excluded = "excluded"
    val Readonly = "readonly"
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
      Readonly, NoInsert, NoUpdate,
      FieldApi, FieldDb, Initial, QuereaseFieldExtrasKey, WabaseFieldExtrasKey)
  }

  lazy val knownFieldExtras = KnownFieldExtras()
  protected val handledFieldExtras = KnownFieldExtras() - KnownFieldExtras.QuereaseFieldExtrasKey

  lazy val knownInlineViewExtras = knownViewExtras ++ knownFieldExtras

  object ViewDefExtrasUtils {
    def getStringSeq(name: String, extras: Map[String, Any]): Seq[String] = {
      getSeq(name, extras) map {
        case s: java.lang.String => s
        case m: java.util.Map[_, _] =>
          if (m.size == 1) m.entrySet.asScala.toList(0).getKey.toString
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
    def getIntExtra(name: String, extras: Map[String, Any]) =
      Option(extras).flatMap(_ get name).map {
        case i: Int => i
        case x => sys.error(
          s"Expecting int value, viewDef, key: ${viewDef.name}, $name")
      }
    def getStringExtra(name: String, extras: Map[String, Any]) =
      Option(extras).flatMap(_ get name).map {
        case s: String => s
        case x => sys.error(
          s"Expecting string value, viewDef, key: ${viewDef.name}, $name")
      }
    def toAuth(viewDef: ViewDef, authPrefix: String) = {
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
    def getExtraOpt(f: FieldDef, key: String) =
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
    def getBooleanExtraOpt(f: FieldDef, key: String) =
      Option(f.extras).flatMap(_ get key).map {
        case b: Boolean => b
        case s: String if s == key => true
        case x => sys.error(
          s"Expecting boolean value or no value, viewDef field, key: ${viewDef.name}.${f.name}, $key")
      }
    def getBooleanExtra(f: FieldDef, key: String) =
      getBooleanExtraOpt(f, key) getOrElse false

  }

  protected def isSortableField(viewDef: ViewDef, f: FieldDef) = {
    FieldDefExtrasUtils.getBooleanExtraOpt(f, KnownFieldExtras.Sortable) getOrElse {
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

      val readonly = getBooleanExtra(f, Readonly)
      val noInsert = getBooleanExtra(f, NoInsert)
      val noUpdate = getBooleanExtra(f, NoUpdate)

      val fieldApiKnownOps = Set(Readonly, NoInsert, NoUpdate, Excluded)
      val fieldDbKnownOps  = Set(Readonly, NoInsert, NoUpdate)
      def getFieldApi(key: String, knownOps: Set[String]) = getStringSeq(key, f.extras) match {
        case api =>
          val ops = api.flatMap(_.trim.split(",").toList).map(_.trim).filter(_ != "").toSet
          val opt = fieldOptionsSelf(f)
          def op(opkey: String) = (ops contains opkey) || api.isEmpty && (opkey match {
            case Excluded => false
            case Readonly => opt != null &&  (opt contains "!")
            case NoInsert => opt != null && !(opt contains "+")
            case NoUpdate => opt != null && !(opt contains "=")
          })
          val unknownOps = ops -- knownOps
          if (unknownOps.nonEmpty)
            sys.error(
              s"Unknown $key value(s), viewDef field ${viewDef.name}.${fieldName}, value(s): ${unknownOps.mkString(", ")}")
          val ro = readonly || op(Readonly)
          FieldApiOps(
            insertable = !ro && !noInsert && !op(Excluded) && !op(NoInsert),
            updatable  = !ro && !noUpdate && !op(Excluded) && !op(NoUpdate),
            excluded   = op(Excluded),
          )
      }

      // TODO "no insert", no update" for complex type (children) - rename, refactor!
      //      [+-=] (i.e. insert, update, delete) is not "no insert" (i.e. update)
      val fieldApi = getFieldApi(FieldApi, fieldApiKnownOps)
      val fieldDb  = getFieldApi(FieldDb,  fieldDbKnownOps) match {
        case api => FieldDbOps(insertable = api.insertable, updatable = api.updatable)
      }

      val required = getBooleanExtra(f, Required)

      val sortable = isSortableField(viewDef, f)
      val hiddenOpt = getBooleanExtraOpt(f, Hidden)
      val visibleOpt = getBooleanExtraOpt(f, Visible)
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
      val persistenceOptions =
        Option(
          if (fieldDb.readonly)
            "!"
          else if (f.type_.isComplexType)
            (fieldDb.insertable, fieldDb.updatable) match {
              case (false, false) => "!"
              case (false, true)  => "=/" + Option(fieldOptionsRef(f)).getOrElse("")
              case (true, false)  => "+/" + Option(fieldOptionsRef(f)).getOrElse("")
              case (true, true)   => Option(fieldOptionsRef(f)).map("/" + _).orNull
            }
          else
            (fieldDb.insertable, fieldDb.updatable) match {
              case (false, false) => "!"
              case (false, true)  => "="
              case (true,  false) => "+"
              case (true,  true)  => null
            }
        ).map(o => s"[$o]")
         .orNull

      import f._
      MojozFieldDef(table, tableAlias, name, alias, persistenceOptions, isOverride, isCollection,
        isExpression, expression, f.saveTo, resolver, nullable,
        type_, enum_, joinToParent, orderBy,
        comments, extras)
      .updateWabaseExtras(_ => AppFieldDef(fieldApi, fieldDb, label, required, sortable, visible))
    }
    import viewDef._
    val auth = toAuth(viewDef, Auth)

    val api = getStringSeq(Api, viewDef.extras) match {
      case Nil => Nil
      case api => api.flatMap(_.trim.split("[\\s,]+").toList).filter(_ != "")
    }
    /*
    // TODO check role set, check method set, check not two roles in a row!
    val unknownApiMethods = api.toSet -- knownApiMethods
    if (unknownApiMethods.size > 0)
      sys.error(
        s"Unknown api method(s), viewDef: ${viewDef.name}, method(s): ${unknownApiMethods.mkString(", ")}")
    */
    val apiMap = api.foldLeft((Map[String, String](), defaultApiRoleName))((mr, x) =>
      if (knownApiMethods contains x) (mr._1 + (x -> mr._2), mr._2) else (mr._1, x.toUpperCase))._1

    val limit = getIntExtra(Limit, viewDef.extras) getOrElse 100
    val cp = getStringExtra(ConnectionPool, viewDef.extras).orNull

    val actions = Action().foldLeft(Map[String, Action]()) { (res, actionName) =>
      val a = parseAction(s"${viewDef.name}.$actionName", getSeq(actionName, viewDef.extras))
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


    MojozViewDef(name, db, table, tableAlias, joins, filter,
      viewDef.groupBy, viewDef.having, orderBy, extends_,
      comments, appFields, viewDef.saveTo, extras)
      .updateWabaseExtras(_ => AppViewDef(limit, cp, auth, apiMap, actions, Map.empty))
  }

  protected def transformAppViewDefs(viewDefs: Map[String, ViewDef]): Map[String, ViewDef] =
    Option(viewDefs)
      // .map(resolveAuth)
      .map(resolveDbAccessKeys)
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

  protected def resolveDbAccessKeys(viewDefs: Map[String, ViewDef]): Map[String, ViewDef] =
    viewDefs.transform { case (viewName, viewDef) =>
      import Action._
      // result is Map of: (viewName, actionName) -> Seq[DbAccessKey]
      def dbAccessKeys(v: ViewDef, action: String, processed: Set[(String, String)]): Map[(String, String), Seq[DbAccessKey]] = {
        if (processed(v.name -> action)) Map()
        else {
          def collectFromAction[A](pf: PartialFunction[Step, A]): Seq[A] = {
            v.actions
              .get(action)
              .map(_.steps)
              .map(_.collect(pf))
              .getOrElse(Nil)
          }

          val viewDbKeys = {
            def dbs(tresql: String) = {
              val p = new QueryParser(null, null)
              p.traverser(p.dbExtractor)(Nil)(p.parseExp(tresql)).map(DbAccessKey(_, null))
            }
            val actionDbs =
              collectFromAction {
                case Evaluation(_, _, Tresql(tresql)) => dbs(tresql)
                case Return(_, _, Tresql(tresql)) => dbs(tresql)
                case Validations(_, _, dbkey)  => dbkey.toList
                case _ => Nil
              } .flatten
            (if (v.db != null || v.cp != null) Seq(DbAccessKey(v.db, v.cp)) else Nil) ++ actionDbs
          }

          val children =
            v.fields
              .collect {
                case f if f.type_.isComplexType =>
                  (f.type_.name, action)
              } ++
              collectFromAction {
                case Evaluation(_, _, ViewCall(m, vn)) if !vn.startsWith(":") && vn != "this" => (vn, m)
                case Return    (_, _, ViewCall(m, vn)) if !vn.startsWith(":") && vn != "this" => (vn, m)
              }

          val new_processed = processed + (v.name -> action)

          children.distinct.foldLeft(Map[(String, String), Seq[DbAccessKey]]((v.name, action) -> viewDbKeys)) {
            case (res, (child_name, act)) =>
              res ++ dbAccessKeys(viewDefs(child_name), act, new_processed)
          }
        }
      }

      val actionToDbAccessKeys = Action().map { case action =>
        val viewAndActionKeys = dbAccessKeys(viewDef, action, Set()).flatMap(_._2).toList.distinct
        // validate db access keys so that one db corresponds only to one connection pool
        viewAndActionKeys.groupBy(_.db).foreach { case (db, gr) =>
          if (gr.size > 1) {
            val pools = gr.map(_.cp)
            sys.error(s"Multiple connection pools - ($pools) for db $db in view $viewName")
          }
        }
        (action, viewAndActionKeys)
      }.toMap

      viewDef.updateWabaseExtras(_.copy(actionToDbAccessKeys = actionToDbAccessKeys))
    }

  protected def parseAction(objectName: String, stepData: Seq[Any]): Action = {
    // matches - 'validations validation_name [db:cp]'
    val validationRegex = new Regex(s"(?U)${Action.ValidationsKey}(?:\\s+(\\w+))?(?:\\s+\\[(?:\\s*(\\w+)?\\s*(?::\\s*(\\w+)\\s*)?)\\])?")
    val namedStepRegex = """(?U)(?:(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)\s*=\s*)?(.+)""".r
    val removeVarStepRegex = """(?U)(?:(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)\s*-=\s*)""".r
    val viewCallRegex = new Regex(Action().mkString("(?U)(", "|", """)\s+(:?\w+)"""))
    val invocationRegex = """(?U)\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*(\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)+""".r
    val setEnvRegex = """setenv\s+(.+)""".r //dot matches new line as well
    val returnRegex = """return\s+(.+)""".r //dot matches new line as well
    val uniqueOpRegex = """unique_opt\s+(.+)""".r
    val redirectToKeyOpRegex = """redirect\s+([_\p{IsLatin}][_\p{IsLatin}0-9]*)""".r
    val redirectOpRegex = """redirect\s+(.+)""".r
    val statusOpRegex = """status(?:\s+(\w+))?(?:\s+(.+))?""".r
    val commitOpRegex = """commit""".r
    val ifOpRegex = """if\s+(.+)""".r
    val foreachOpRegex = """foreach\s+(.+)""".r
    val fileOpRegex = """file\s+(.+)""".r
    val toFileOpRegex = """to file\s+(.+)""".r
    /* [jobs]
    val jobCallRegex = """(?U)job\s+(:?\w+)""".r
    [jobs] */
    def isViewCall(st: String) = viewCallRegex.pattern.matcher(st).matches
    def isInvocation(st: String) = invocationRegex.pattern.matcher(st).matches
    import ViewDefExtrasUtils._
    val steps = stepData.map { step =>
      def parseOp(st: String): Action.Op = {
        def statusParameterIdx(exp: String) = if (exp == null) -1 else {
          val p = parser
          p.traverser[Int](idx => {
            case Cols(_, cols) =>
              cols indexWhere {
                case Col(Const("?"), null) => true
                case _ => false
              }
          })(-1)(p.parseExp(exp))
        }
        if (isViewCall(st)) {
          val viewCallRegex(method, view) = st
          Action.ViewCall(method, view)
        } else if (isInvocation(st)) {
          val idx = st.lastIndexOf('.')
          Action.Invocation(st.substring(0, idx), st.substring(idx + 1))
        /* [jobs]
        } else if (jobCallRegex.pattern.matcher(st).matches()) {
          val jobCallRegex(jobName) = st
          Action.JobCall(jobName)
        [jobs] */
        } else if (uniqueOpRegex.pattern.matcher(st).matches()) {
          val uniqueOpRegex(inner) = st
          Action.UniqueOpt(parseOp(inner))
        } else if (redirectToKeyOpRegex.pattern.matcher(st).matches()) {
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
        } else if (fileOpRegex.pattern.matcher(st).matches()) {
          val fileOpRegex(idShaTresql) = st
          Action.File(idShaTresql)
        } else if (toFileOpRegex.pattern.matcher(st).matches()) {
          val toFileOpRegex(ops) = st
          val idx = ops.indexOf(",")
          if (idx == -1) { // no name tresql and content type
            Action.ToFile(parseOp(ops), null, null)
          } else {
            def parseToFile(s: String) = parser.parseExp(s) match {
              case org.tresql.parsing.Arr(List(a)) => (a.tresql, null, null)
              case org.tresql.parsing.Arr(List(a, b)) => (a.tresql, b.tresql, null)
              case org.tresql.parsing.Arr(List(a, b, c)) => (a.tresql, b.tresql, c.tresql)
              case _ => (s, null, null)
            }
            val op = ops.substring(0, idx)
            if (isViewCall(op) || isInvocation(op)) {
              val (fileName, contentType, _) = parseToFile(ops.substring(idx + 1))
              Action.ToFile(parseOp(op), fileName, contentType)
            } else {
              val (tresql, fileName, contentType) = parseToFile(op)
              Action.ToFile(Action.Tresql(tresql), fileName, contentType)
            }
          }
        } else if (commitOpRegex.pattern.matcher(st).matches()) {
          Action.Commit
        } else {
          Action.Tresql(st)
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
            val removeVarStepRegex(name) = s
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
                if (db == null && cp == null) None else Option(DbAccessKey(db, cp))
              )
            } else {
              value match {
                case jm: java.util.Map[String@unchecked, _] =>
                  // may be 'if' or 'foreach' step
                  parseStep(jm) match {
                    case e: Action.Evaluation => e.copy(name = Option(name))
                    case x => sys.error(s"Invalid step '$name' value here: $x")
                  }
                case al: java.util.ArrayList[_] if name != null =>
                  // 'if' or 'foreach' step
                  val op =
                    if (ifOpRegex.pattern.matcher(name).matches()) {
                      val ifOpRegex(condOpSt) = name
                      Action.If(parseOp(condOpSt), parseAction(objectName, al.asScala.toList))
                    } else if (foreachOpRegex.pattern.matcher(name).matches()) {
                      val foreachOpRegex(initOpSt) = name
                      Action.Foreach(parseOp(initOpSt), parseAction(objectName, al.asScala.toList))
                    } else sys.error(s"'$objectName' parsing error, invalid value: '$name'. " +
                      s"Only 'if' of 'foreach' operations allowed.")
                  Action.Evaluation(None, Nil, op)
                case x => parseStringStep(Option(name), x.toString)
              }
            }
          case x =>
            sys.error(s"'$objectName' parsing error, invalid value: $x")
        }
      }
      parseStep(step)
    }.toList
    Action(steps)
  }

  /* [jobs]
  protected def parseJob(job: Map[String, Any]): Job = {
    val name = job.get("name").map(String.valueOf)
      .getOrElse(sys.error(s"Error parsing job, missing 'name' attribute"))
    val schedule = job.get("schedule").map(String.valueOf)
    val condition = job.get("condition").map(String.valueOf)
    val action = parseAction(name, ViewDefExtrasUtils.getSeq("action", job))
    Job(name, schedule, condition, action)
  }
  [jobs] */
}


object AppMetadata {

  case class AuthFilters(
    forGet: Seq[String],
    forList: Seq[String],
    forInsert: Seq[String],
    forUpdate: Seq[String],
    forDelete: Seq[String]
  )

  val AuthEmpty = AuthFilters(Nil, Nil, Nil, Nil, Nil)

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
    val OffsetKey = "offset"
    val LimitKey  = "limit"
    val OrderKey  = "sort"

    val BindVarCursorsFunctionName = "build_cursors"
    val BindVarCursorsForViewFunctionName = "build_cursors_for_view"

    sealed trait Op
    sealed trait Step {
      def name: Option[String]
    }

    case class VariableTransform(form: String, to: Option[String])

    case class Tresql(tresql: String) extends Op
    case class ViewCall(method: String, view: String) extends Op
    case class RedirectToKey(name: String) extends Op
    case class UniqueOpt(innerOp: Op) extends Op
    case class Invocation(className: String, function: String) extends Op
    case class Status(code: Option[Int], bodyTresql: String, parameterIndex: Int) extends Op
    case class VariableTransforms(transforms: List[VariableTransform]) extends Op
    case class Foreach(initOp: Op, action: Action) extends Op
    case class If(cond: Op, action: Action) extends Op
    case class File(idShaTresql: String) extends Op
    case class ToFile(contentOp: Op, nameTresql: String, contentTypeTresql: String) extends Op
    /* [jobs]
    case class JobCall(name: String) extends Op
    [jobs] */
    case object Commit extends Op

    case class Evaluation(name: Option[String], varTrans: List[VariableTransform], op: Op) extends Step
    case class SetEnv(name: Option[String], varTrans: List[VariableTransform], value: Op) extends Step
    case class Return(name: Option[String], varTrans: List[VariableTransform], value: Op) extends Step
    case class Validations(name: Option[String], validations: Seq[String], db: Option[DbAccessKey]) extends Step
    case class RemoveVar(name: Option[String]) extends Step
  }

  case class Action(steps: List[Action.Step])

  /** Database name (as used in mojoz metadata) and corresponding connection pool name */
  case class DbAccessKey(
    db: String,
    cp: String,
  )

  /* [jobs]
  case class Job(name: String,
                 schedule: Option[String],
                 condition: Option[String],
                 action: Action)
  [jobs] */

  trait AppViewDefExtras {
    val limit: Int
    val cp: String
    val auth: AuthFilters
    val apiMethodToRole: Map[String, String]
    val actions: Map[String, Action]
    val actionToDbAccessKeys: Map[String, Seq[DbAccessKey]]
  }

  private [wabase] case class AppViewDef(
    limit: Int = 1000,
    cp: String = null,
    auth: AuthFilters = AuthFilters(Nil, Nil, Nil, Nil, Nil),
    apiMethodToRole: Map[String, String] = Map(),
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

  case class FieldDbOps(
    insertable: Boolean,
    updatable: Boolean,
  ) {
    val readonly = !insertable && !updatable
  }

  trait AppFieldDefExtras {
    val api: FieldApiOps
    val db: FieldDbOps
    val label: String
    val required: Boolean
    val sortable: Boolean
    val visible: Boolean
  }

  private [wabase] case class AppFieldDef(
    api: FieldApiOps  = FieldApiOps(insertable = false, updatable = false, excluded = true),
    db:  FieldDbOps   = FieldDbOps (insertable = false, updatable = false),
    label: String = null,
    required: Boolean = false,
    sortable: Boolean = false,
    visible:  Boolean = false,
  ) extends AppFieldDefExtras

  val WabaseViewExtrasKey = "wabase-view-extras"
  val WabaseFieldExtrasKey = "wabase-field-extras"
  implicit class AugmentedAppViewDef(viewDef: AppMetadata#ViewDef) extends QuereaseMetadata.AugmentedQuereaseViewDef(viewDef) with AppViewDefExtras {
    private val defaultExtras = AppViewDef()
    private val appExtras = extras(WabaseViewExtrasKey, defaultExtras)
    override val limit = appExtras.limit
    override val cp = appExtras.cp
    override val auth = appExtras.auth
    override val apiMethodToRole = appExtras.apiMethodToRole
    override val actions = appExtras.actions
    override val actionToDbAccessKeys = appExtras.actionToDbAccessKeys
    def updateWabaseExtras(updater: AppViewDef => AppViewDef): AppMetadata#ViewDef =
      updateExtras(WabaseViewExtrasKey, updater, defaultExtras)

    override protected def updateExtrasMap(extras: Map[String, Any]) = viewDef.copy(extras = extras)
    override protected def extrasMap = viewDef.extras
  }
  implicit class AugmentedAppFieldDef(fieldDef: AppMetadata#FieldDef) extends QuereaseMetadata.AugmentedQuereaseFieldDef(fieldDef) with AppFieldDefExtras {
    private val defaultExtras = AppFieldDef()
    val appExtras = extras(WabaseFieldExtrasKey, defaultExtras)
    override val api = appExtras.api
    override val db = appExtras.db
    override val label = appExtras.label
    override val required = appExtras.required
    override val sortable = appExtras.sortable
    override val visible = appExtras.visible
    def updateWabaseExtras(updater: AppFieldDef => AppFieldDef): AppMetadata#FieldDef =
      updateExtras(WabaseFieldExtrasKey, updater, defaultExtras)

    override protected def updateExtrasMap(extras: Map[String, Any]): Any = fieldDef.copy(extras = extras)
    override protected def extrasMap = fieldDef.extras
  }

  trait AppMdConventions extends MdConventions {
    def isIntegerName(name: String) = false
    def isDecimalName(name: String) = false
  }

  import MdConventions._
  class DefaultAppMdConventions(
    integerNamePatternStrings: Seq[String] =
      namePatternsFromResource("/md-conventions/integer-name-patterns.txt", Nil),
    decimalNamePatternStrings: Seq[String] =
      namePatternsFromResource("/md-conventions/decimal-name-patterns.txt", Nil)
  ) extends SimplePatternMdConventions with AppMdConventions {

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
}
