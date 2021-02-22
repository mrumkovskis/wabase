package org.wabase

import org.mojoz.metadata._
import org.mojoz.metadata.{ViewDef => MojozViewDef}
import org.mojoz.metadata.{FieldDef => MojozFieldDef}
import org.mojoz.metadata.in._
import org.mojoz.metadata.io.MdConventions
import org.mojoz.metadata.out.SqlGenerator.SimpleConstraintNamingRules
import org.mojoz.querease._
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

trait AppMetadata extends QuereaseMetadata { this: AppQuerease =>

  import AppMetadata._

  override lazy val metadataConventions: AppMdConventions = new DefaultAppMdConventions
  override lazy val nameToViewDef: Map[String, ViewDef] = {
    val mojozViewDefs =
      YamlViewDefLoader(tableMetadata, yamlMetadata, tresqlJoinsParser, metadataConventions, Seq("api"))
        .nameToViewDef
    toAppViewDefs(mojozViewDefs)
  }
  def toAppViewDefs(mojozViewDefs: Map[String, ViewDef]) = resolveAuth {
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

  val authFieldNames = Set("is_update_relevant", "is_delete_relevant")

  val knownApiMethods = Set("get", "list", "chunked_list", "save", "delete")
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
    val Limit = "limit"
    val Validations = "validations"
    val ConnectionPool = "cp"
    val QuereaseViewExtrasKey = QuereaseMetadata.QuereaseViewExtrasKey
    val WabaseViewExtrasKey = AppMetadata.WabaseViewExtrasKey
    def apply() =
      Set(Api, Auth, Limit, Validations, ConnectionPool, QuereaseViewExtrasKey, WabaseViewExtrasKey) ++
        Action()
  }

  lazy val knownViewExtras = KnownViewExtras()
  protected val handledViewExtras = KnownViewExtras() - KnownViewExtras.QuereaseViewExtrasKey

  lazy val knownPrefixes = Set(KnownViewExtras.Auth)

  object KnownFieldExtras {
    val FieldApi = "field api" // avoid name clash with "api"
    val FieldDb = "field db"

    val None_ = "none"

    val NoGet = "no get"
    val NoSave = "no save"
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
      NoGet, NoSave, Readonly, NoInsert, NoUpdate,
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
    def fieldLabelFromName(f: FieldDef) = Option(f.alias).orElse(Option(f.name)).map(fieldNameToLabel).get
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

      val noGet = getBooleanExtra(f, NoGet)
      val readonly = getBooleanExtra(f, Readonly) || getBooleanExtra(f, NoSave)
      val noInsert = getBooleanExtra(f, NoInsert)
      val noUpdate = getBooleanExtra(f, NoUpdate)

      val fieldApiKnownOps = // TODO rename, refine?
        Set(None_, NoGet, NoSave, Readonly, NoInsert, NoUpdate)
      def getFieldApi(key: String) = getStringSeq(key, f.extras) match {
        case api =>
          val ops = api.flatMap(_.trim.split(",").toList).map(_.trim).filter(_ != "").toSet
          def op(key: String) = ops contains key
          val unknownFieldApiMethods = ops -- fieldApiKnownOps
          if (unknownFieldApiMethods.nonEmpty)
            sys.error(
              s"Unknown field api method(s), viewDef: ${viewDef.name}, method(s): ${unknownFieldApiMethods.mkString(", ")}")
          val noOps = ops.isEmpty && api.nonEmpty || op(None_)
          val ro = readonly || op(Readonly) || op(NoSave)
          FieldOps(
            !noOps && !op(NoGet) && !(f.isExpression && f.expression == null && key == FieldDb),
            !noOps && !ro && !noInsert && !op(NoInsert),
            !noOps && !ro && !noUpdate && !op(NoUpdate))
      }

      val fieldApi = getFieldApi(FieldApi)
      val fieldDb = getFieldApi(FieldDb)

      val required = getBooleanExtra(f, Required)

      val sortable =
        getBooleanExtraOpt(f, Sortable) getOrElse {
          if (f.isExpression || f.isCollection || viewDef.table == null) false
          else tableMetadata.tableDef(viewDef.table).idx.exists(_.cols(0) == f.name)
        }

      val hiddenOpt = getBooleanExtraOpt(f, Hidden)
      val visibleOpt = getBooleanExtraOpt(f, Visible)
      if (hiddenOpt.isDefined && visibleOpt.isDefined && hiddenOpt == visibleOpt)
        sys.error(s"Conflicting values of visible and hidden, viewDef field: ${viewDef.name}.${f.name}")
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
          s"Unknown or misplaced properties for viewDef field ${viewDef.name}.${f.name}: ${unknownKeys.mkString(", ")}")
      val isExpression = if (fieldDb.gettable) f.isExpression else true
      val expression = if (fieldDb.gettable) f.expression else null
      val persistenceOptions = Option(f.options) getOrElse ""
      import f._
      MojozFieldDef(table, tableAlias, name, alias, persistenceOptions, isOverride, isCollection,
        isExpression, expression, f.saveTo, resolver, nullable,
        type_, enum, joinToParent, orderBy,
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
    val cp = getStringExtra(ConnectionPool, viewDef.extras) getOrElse DEFAULT_CP.connectionPoolName
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

    def createRelevanceField(name: String, expression: String): FieldDef =
      (new MojozFieldDef(name, Type("boolean", None, None, None, false)))
        .copy(options = "", isExpression = true, expression = expression)
    val appView = MojozViewDef(name, table, tableAlias, joins, filter,
      viewDef.groupBy, viewDef.having, orderBy, extends_,
      comments, appFields, viewDef.saveTo, extras).updateWabaseExtras(_ => AppViewDef(limit, cp, auth, apiMap))
    def hasAuthFilter(viewDef: ViewDef): Boolean = viewDef.auth match {
      case AuthFilters(g, l, i, u, d) =>
        !(g.isEmpty && l.isEmpty && i.isEmpty && u.isEmpty && d.isEmpty)
    }
    if (hasAuthFilter(appView)) {
      appView.copy(fields = appView.fields ++
        (if (appView.auth.forUpdate.isEmpty) Nil
        else createRelevanceField("is_update_relevant",
          appView.auth.forUpdate.map(a => s"($a)").mkString(" & ")) :: Nil) ++ //do not use List(..) because it conflicts with KnownAuthOps.List
        (if (appView.auth.forDelete.isEmpty) Nil
        else createRelevanceField("is_delete_relevant",
          appView.auth.forDelete.map(a => s"($a)").mkString(" & ")) :: Nil)) //do not use List(..) because it conflicts with KnownAuthOps.List
    } else appView
  }

  /* Sets field options to horizontal auth statements if such are defined for child view, so that during ort auth
   * for child views are applied.
   */
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
    val Get = "get"
    val List = "list"
    val Save = "save"
    val Delete = "delete"
    val Create = "create"
    def apply() =
      Set(Get, List, Save, Delete, Create)

    trait Step

    case class Tresql(name: String, tresql: String) extends Step
    case class ViewCall(name: String, view: String, method: String, params: Option[String])
    case class Validations(validations: Seq[String]) extends Step
    case class Invocation(function: String) extends Step
    case class Return(values: List[String]) extends Step
  }

  case class Action(steps: List[Action.Step])


  trait AppViewDefExtras {
    val limit: Int
    val cp: String
    val auth: AuthFilters
    val apiMethodToRole: Map[String, String]
    val actions: Map[String, Action]
  }

  private [wabase] case class AppViewDef(
    limit: Int = 1000,
    cp: String = DEFAULT_CP.connectionPoolName,
    auth: AuthFilters = AuthFilters(Nil, Nil, Nil, Nil, Nil),
    apiMethodToRole: Map[String, String] = Map(),
    actions: Map[String, Action] = Map()
  ) extends AppViewDefExtras

  case class FieldOps(
    gettable: Boolean,
    insertable: Boolean,
    updatable: Boolean)

  trait AppFieldDefExtras {
    val api: FieldOps
    val db: FieldOps
    val label: String
    val required: Boolean
    val sortable: Boolean
    val visible: Boolean
  }

  private [wabase] case class AppFieldDef(
    api: FieldOps = FieldOps(true, false, false),
    db: FieldOps = FieldOps(true, false, false),
    label: String = null,
    required: Boolean = false,
    sortable: Boolean = false,
    visible: Boolean = false,
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
