package org.wabase

import org.mojoz.metadata.ViewDef
import org.mojoz.metadata.FieldDef
import org.mojoz.metadata.in._
import org.mojoz.metadata.io.MdConventions
import org.mojoz.metadata.out.SqlGenerator.SimpleConstraintNamingRules
import org.mojoz.querease._
import org.tresql.{CacheBase, SimpleCacheBase}
import org.tresql.ast.{Exp, Variable}
import org.tresql.parsing.QueryParsers
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
    val NoDb = "no db"
    val QuereaseViewExtrasKey = QuereaseMetadata.QuereaseViewExtrasKey
    val WabaseViewExtrasKey = AppMetadata.WabaseViewExtrasKey
    def apply() =
      Set(Api, Auth, Key, Limit, Validations, ConnectionPool, NoDb, QuereaseViewExtrasKey, WabaseViewExtrasKey) ++
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
    def getBooleanExtraOpt(name: String, extras: Map[String, Any]) =
      Option(extras).flatMap(_ get name).map {
        case b: Boolean => b
        case s: String if s == name => true
        case x => sys.error(
          s"Expecting boolean value or no value, viewDef, key: ${viewDef.name}.$name")
      }
    def getBooleanExtra(name: String, extras: Map[String, Any]) =
      getBooleanExtraOpt(name, extras) getOrElse false

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
      FieldDef(table, tableAlias, name, alias, persistenceOptions, isOverride, isCollection,
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
    val nodb = getBooleanExtra(NoDb, viewDef.extras)

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


    ViewDef(name, db, table, tableAlias, joins, filter,
      viewDef.groupBy, viewDef.having, orderBy, extends_,
      comments, appFields, viewDef.saveTo, extras)
      .updateWabaseExtras(_ => AppViewDef(limit, cp, nodb, auth, apiMap, actions, Map.empty))
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

  protected def extractViewVariables(viewDefs: Map[String, ViewDef])(action: String, viewName: String): Seq[Variable] = {
    import Action._
    import TresqlExtraction._
    val varExtractor = parser.variableExtractor(Nil)

    lazy val opTresqlTrav: OpTresqlTraverser[(Seq[Variable], Set[String])] =
      opTresqlTraverser(opTresqlTrav, stepTresqlTrav)(_ => PartialFunction.empty)

    lazy val stepTresqlTrav: StepTresqlTraverser[(Seq[Variable], Set[String])] =
      stepTresqlTraverser(opTresqlTrav) { state =>
        def trav = opTresqlTrav(state)
        def us(st: Step, s: State[(Seq[Variable], Set[String])]) = { st.name
          .map(n => s.copy(value = s.value._1 -> (s.value._2 + n)))
          .getOrElse(s)
        }
        {
          case s: Evaluation => us(s, trav(s.op))
          case s: SetEnv => us(s, trav(s.value))
          case s: Return => us(s, trav(s.value))
        }
      }

    val state = State[(Seq[Variable], Set[String])](action, viewName, viewDefs, parser,
      tresqlExtractor = vars => {
        case e if varExtractor.isDefinedAt(e) =>
          val actionDefinedVars = vars._2
          (vars._1 ++ varExtractor(e).filterNot(v => actionDefinedVars(v.variable))) -> actionDefinedVars
      },
      viewExtractor = vars => vd => vars, // TODO extract variables from view, move AppBase.filterParams function here?
      processed = Set(), value = (Nil, Set())
    )
    processView(stepTresqlTrav)(state).value._1.distinct
  }

  protected def resolveDbAccessKeys(viewDefs: Map[String, ViewDef]): Map[String, ViewDef] =
    viewDefs.transform { case (viewName, viewDef) =>
      import Action._
      import TresqlExtraction._
      val actionToDbAccessKeys = Action().map { case action =>
        val dbExtractor = parser.dbExtractor(Nil)

        lazy val opTresqlTrav: OpTresqlTraverser[Seq[DbAccessKey]] =
          opTresqlTraverser(opTresqlTrav, stepTresqlTrav)(_ => PartialFunction.empty)

        lazy val stepTresqlTrav: StepTresqlTraverser[Seq[DbAccessKey]] =
          stepTresqlTraverser(opTresqlTrav) { state =>
            { case Validations(_, _, dbkey) => state.copy(value = state.value ++ dbkey.toList) }
          }

        val state = State[Seq[DbAccessKey]](action, viewName, viewDefs, parser,
          tresqlExtractor = dbKeys => {
            case e if dbExtractor.isDefinedAt(e) => dbKeys ++ dbExtractor(e).map(DbAccessKey(_, null))
          },
          viewExtractor = dbkeys => vd =>
            dbkeys ++ (if (vd.db != null || vd.cp != null) Seq(DbAccessKey(vd.db, vd.cp)) else Nil),
          processed = Set(), value = Nil
        )

        val dbkeys = processView(stepTresqlTrav)(state).value.distinct
        // validate db access keys so that one db corresponds only to one connection pool
        dbkeys.groupBy(_.db).foreach { case (db, gr) =>
          if (gr.size > 1) {
            val pools = gr.map(_.cp)
            sys.error(s"Multiple connection pools - ($pools) for db $db in view $viewName")
          }
        }
        (action, dbkeys)
      }.toMap
      viewDef.updateWabaseExtras(_.copy(actionToDbAccessKeys = actionToDbAccessKeys))
    }

  protected def parseAction(objectName: String, stepData: Seq[Any]): Action = {
    val namedStepRegex = """(?U)(?:(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)\s*=\s*)?(.+)""".r
    // matches - 'validations validation_name [db:cp]'
    val validationRegex = new Regex(s"(?U)${Action.ValidationsKey}(?:\\s+(\\w+))?(?:\\s+\\[(?:\\s*(\\w+)?\\s*(?::\\s*(\\w+)\\s*)?)\\])?")
    val dbUseRegex = new Regex(s"${Action.DbUseKey}")
    val transactionRegex = new Regex(s"${Action.TransactionKey}")
    val removeVarStepRegex = """(?U)(?:(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)\s*-=\s*)""".r
    val setEnvRegex = """setenv\s+(.+)""".r //dot matches new line as well
    val returnRegex = """return\s+(.+)""".r //dot matches new line as well
    val redirectToKeyOpRegex = """redirect\s+([_\p{IsLatin}][_\p{IsLatin}0-9]*)""".r
    val redirectOpRegex = """redirect\s+(.+)""".r
    val statusOpRegex = """status(?:\s+(\w+))?(?:\s+(.+))?""".r
    val commitOpRegex = """commit""".r
    val ifOpRegex = """if\s+(.+)""".r
    val foreachOpRegex = """foreach\s+(.+)""".r
    val confRegex = {
      val confPropRegex = """\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*(?:\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*+)*"""
      val typeNames = Action.ConfTypes.types.map(_.name).mkString("|")
      new Regex(s"(?U)conf(?:\\s+($typeNames))?\\s+($confPropRegex)")
    }
    /* [jobs]
    val jobCallRegex = """(?U)job\s+(:?\w+)""".r
    [jobs] */
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
        } else if (confRegex.pattern.matcher(st).matches()) {
          val confRegex(t, n) = st
          Action.Conf(n, Action.ConfTypes.parse(t))
        } else if (commitOpRegex.pattern.matcher(st).matches()) {
          Action.Commit
        } else {
          dataFlowParser.parseOperation(st)
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
                  // may be 'if', 'foreach', 'db ...' step
                  parseStep(jm) match {
                    case e: Action.Evaluation => e.copy(name = Option(name))
                    case x => sys.error(s"Invalid step '$name' value here: $x")
                  }
                case al: java.util.ArrayList[_] if name != null =>
                  // 'if', 'foreach', 'db ...' step
                  val op =
                    if (ifOpRegex.pattern.matcher(name).matches()) {
                      val ifOpRegex(condOpSt) = name
                      Action.If(parseOp(condOpSt), parseAction(objectName, al.asScala.toList))
                    } else if (foreachOpRegex.pattern.matcher(name).matches()) {
                      val foreachOpRegex(initOpSt) = name
                      Action.Foreach(parseOp(initOpSt), parseAction(objectName, al.asScala.toList))
                    } else if (dbUseRegex.pattern.matcher(name).matches()) {
                      Action.Db(parseAction(objectName, al.asScala.toList), true)
                    } else if (transactionRegex.pattern.matcher(name).matches()) {
                      Action.Db(parseAction(objectName, al.asScala.toList), false)
                    } else sys.error(s"'$objectName' parsing error, invalid value: '$name'. " +
                      s"Only 'if', 'foreach', 'db use', 'transaction' operations allowed.")
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

  // lazy val so that tresqlUri is initialized when dataFlowParser is referenced
  protected lazy val dataFlowParser: AppMetadataDataFlowParser =
    new CachedAppMetadataDataFlowParser(4096, tresqlUri)

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

// TODO add macros from resources so that saved parser cache can be used later in runtime
trait AppMetadataDataFlowParser extends QueryParsers { self =>
  import AppMetadata.Action._
  import AppMetadata.Action

  val ActionRegex = new Regex(Action().mkString("(?U)(", "|", """)"""))
  val InvocationRegex = """(?U)\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*(\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}*)+""".r
  val ViewNameRegex = "(?U)\\w+".r

  protected def cache: CacheBase[Op] = null
  protected def tresqlUri: TresqlUri

  def parseOperation(op: String): Op = {
    def parseOp = phrase(operation)(new scala.util.parsing.input.CharSequenceReader(op)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }
    if (cache != null) {
      cache.get(op).getOrElse {
        val pop = parseOp
        cache.put(op, pop)
        pop
      }
    } else parseOp
  }

  def tresqlOp: Parser[Tresql] = expr ^^ { e => Tresql(e.tresql) }
  def viewOp: Parser[ViewCall] = ActionRegex ~ ViewNameRegex ~ opt(operation) ^^ {
    case action ~ view ~ op => ViewCall(action, view, op.orNull)
  }
  def uniqueOptOp: Parser[UniqueOpt] = "unique_opt" ~> operation ^^ UniqueOpt
  def invocationOp: Parser[Invocation] = opt(opResultType) ~ InvocationRegex ^^ {
    case rt ~ res =>
      val idx = res.lastIndexOf('.')
      Action.Invocation(res.substring(0, idx), res.substring(idx + 1), rt)
  }
  def fileOp: Parser[File] = opt(opResultType) ~ ("file" ~> expr) ^^ {
    case conformTo ~ e => File(e.tresql, conformTo)
  }
  def toFileOp: Parser[ToFile] = "to file" ~> operation ~ opt(expr) ~ opt(expr) ^^ {
    case op ~ fileName ~ contentType =>
      ToFile(op, fileName.map(_.tresql).orNull, contentType.map(_.tresql).orNull)
  }
  def httpOp: Parser[Http] = {
    def tu(uri: Exp) = TresqlUri.Tresql(uri.tresql, tresqlUri.queryStringColIdx(uri)(self))
    def http_get_delete: Parser[Http] =
      "http" ~> opt("get" | "delete") ~ bracesTresql ~ opt(expr) ^^ {
        case method ~ uri ~ headers =>
          Http(method.getOrElse("get"), tu(uri), headers.map(_.tresql).orNull, null)
      }
    def http_post_put: Parser[Http] =
      "http" ~> ("post" | "put") ~ bracesTresql ~ operation ~ opt(expr) ^^ {
        case method ~ uri ~ op ~ headers =>
          Http(method, tu(uri), headers.map(_.tresql).orNull, op )
      }
    opt(opResultType) ~ (http_post_put | http_get_delete) ^^ {
      case conformTo ~ http => http.copy(conformTo = conformTo)
    }
  }
  def bracesOp: Parser[Op] = "(" ~> operation <~ ")"
  def bracesTresql: Parser[Exp] = ("(" ~> expr <~ ")") | expr
  def operation: Parser[Op] = viewOp | uniqueOptOp | invocationOp | httpOp | fileOp | toFileOp |
    bracesOp | tresqlOp

  private def opResultType: Parser[OpResultType] = {
    sealed trait ResType
    case object NoType extends ResType
    case class ViewType(vn: String) extends ResType
    def noType: Parser[ResType] = "map" ^^^ NoType
    def viewType: Parser[ResType] = opt("`") ~> ViewNameRegex <~ opt("`") ^^ ViewType

    "as" ~> (noType | (opt("`") ~> viewType <~ opt("`"))) ~ opt("*") ^^ {
      case ct ~ coll => OpResultType(ct match { case NoType => null case ViewType(vn) => vn }, coll.nonEmpty)
    }
  }
}

class CachedAppMetadataDataFlowParser(maxCacheSize: Int, override val tresqlUri: TresqlUri)
  extends AppMetadataDataFlowParser {
  import AppMetadata.Action.Op
  override protected val cache: CacheBase[Op] = new SimpleCacheBase[Op](maxCacheSize)
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
    val DbUseKey = "db use"
    val TransactionKey = "transaction"
    val OffsetKey = "offset"
    val LimitKey  = "limit"
    val OrderKey  = "sort"

    val BindVarCursorsFunctionName = "build_cursors"
    val BindVarCursorsForViewFunctionName = "build_cursors_for_view"

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
    sealed trait Step {
      def name: Option[String]
    }

    case class VariableTransform(form: String, to: Option[String])
    case class OpResultType(viewName: String = null, isCollection: Boolean = false)

    case class Tresql(tresql: String) extends Op
    case class ViewCall(method: String, view: String, data: Op = null) extends Op
    case class RedirectToKey(name: String) extends Op
    case class UniqueOpt(innerOp: Op) extends Op
    case class Invocation(className: String, function: String, conformTo: Option[OpResultType] = None) extends Op
    case class Status(code: Option[Int], bodyTresql: String, parameterIndex: Int) extends Op
    case class VariableTransforms(transforms: List[VariableTransform]) extends Op
    case class Foreach(initOp: Op, action: Action) extends Op
    case class If(cond: Op, action: Action) extends Op
    case class File(idShaTresql: String, conformTo: Option[OpResultType] = None) extends Op
    case class ToFile(contentOp: Op, nameTresql: String = null, contentTypeTresql: String = null) extends Op
    case class Http(method: String,
                    uriTresql: TresqlUri.Tresql,
                    headerTresql: String = null,
                    body: Op = null,
                    conformTo: Option[OpResultType] = None) extends Op
    case class Db(action: Action, doRollback: Boolean) extends Op
    case class Conf(param: String, paramType: ConfType = null) extends Op

    /* [jobs]
    case class JobCall(name: String) extends Op
    [jobs] */
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
        case _: Tresql | _: RedirectToKey | _: Invocation | _: Status |
             _: VariableTransforms | _: File | _: Conf | Commit | null => state
        case o: ViewCall => opTrav(state)(o.data)
        case UniqueOpt(o) => opTrav(state)(o)
        case Foreach(o, a) => traverseAction(a)(stepTrav)(opTrav(state)(o))
        case If(o, a) => traverseAction(a)(stepTrav)(opTrav(state)(o))
        case o: ToFile => opTrav(state)(o.contentOp)
        case o: Http => opTrav(state)(o.body)
        case Db(a, _) => traverseAction(a)(stepTrav)(state)
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
      type TresqlExtractor[T] = T => PartialFunction[Exp, T]
      type StepTresqlTraverser[T] = StepTraverser[State[T]]
      type OpTresqlTraverser[T] = OpTraverser[State[T]]

      case class State[T](action: String, viewName: String, viewDefs: Map[String, ViewDef],
                          parser: QueryParsers, tresqlExtractor: TresqlExtractor[T],
                          viewExtractor: ViewExtractor[T],
                          processed: Set[(String, String)], value: T)


      def processView[T](stepTresqlTrav: => StepTresqlTraverser[T])(s: State[T]): State[T] = {
        if (s.processed(s.action -> s.viewName)) s else {
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

          val newVal = process(s.viewName, s.value, Set())
          val s1 = s.copy(value = newVal, processed = s.processed + (s.action -> s.viewName))
          val vd = s1.viewDefs(s.viewName)
          vd.actions.get(s1.action).map { a =>
            traverseAction(a)(stepTresqlTrav)(s1)
          }.getOrElse(s1)
        }
      }

      private def newValFromTresql[T](parser: QueryParsers,
                                      extr: TresqlExtractor[T])(oldVal: T)(tresql: String) =
        if (tresql == null) oldVal
        else parser.traverser(extr)(oldVal)(parser.parseExp(tresql))

      def opTresqlTraverser[T](opTresqlTrav: => OpTresqlTraverser[T],
        stepTresqlTrav: => StepTresqlTraverser[T])(extractor: OpTresqlTraverser[T]):
          OpTresqlTraverser[T] = {
        def traverse(state: State[T]): PartialFunction[Op, State[T]]= {
          val nv: T => String => T = newValFromTresql[T](state.parser, state.tresqlExtractor)
          def opTrTr = opTresqlTrav(state)
          def us(s: State[T], v: T) = {
            s.copy(value = v)
          }
          {
            case Tresql(tresql) => us(state, nv(state.value)(tresql))
            case Status(_, bodyTresql, _) => us(state, nv(state.value)(bodyTresql))
            case File(idShaTresql, _) => us(state, nv(state.value)(idShaTresql))
            case ToFile(contentOp, nameTresql, contentTypeTresql) =>
              val s1 = opTrTr(contentOp)
              val s2 = us(s1, nv(s1.value)(nameTresql))
              us(s2, nv(s2.value)(contentTypeTresql))
            case Http(_, uriTresql, headerTresql, body, _) =>
              val s1 = us(state, nv(state.value)(uriTresql.uriTresql))
              val s2 = us(s1, nv(s1.value)(headerTresql))
              opTresqlTrav(s2)(body)
            case ViewCall(method, view, data) =>
              val vn = if (view == "this") state.viewName else view
              val ns = opTrTr(data)
              processView(stepTresqlTrav)(ns.copy(action = method, viewName = vn))
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
                newValFromTresql[T](state.parser, state.tresqlExtractor)(v)(s)
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
    val noDb: Boolean
    val auth: AuthFilters
    val apiMethodToRole: Map[String, String]
    val actions: Map[String, Action]
    val actionToDbAccessKeys: Map[String, Seq[DbAccessKey]]
  }

  private [wabase] case class AppViewDef(
    limit: Int = 1000,
    cp: String = null,
    noDb: Boolean = false,
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
  implicit class AugmentedAppViewDef(viewDef: ViewDef)
         extends QuereaseMetadata.AugmentedQuereaseViewDef(viewDef)
            with AppViewDefExtras {
    private val defaultExtras = AppViewDef()
    private val appExtras = extras(WabaseViewExtrasKey, defaultExtras)
    override val limit = appExtras.limit
    override val cp = appExtras.cp
    override val noDb = appExtras.noDb
    override val auth = appExtras.auth
    override val apiMethodToRole = appExtras.apiMethodToRole
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
    override val db = appExtras.db
    override val label = appExtras.label
    override val required = appExtras.required
    override val sortable = appExtras.sortable
    override val visible = appExtras.visible
    def updateWabaseExtras(updater: AppFieldDef => AppFieldDef): FieldDef =
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
