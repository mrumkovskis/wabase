package org.wabase

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine
import org.graalvm.polyglot.HostAccess.Export

import javax.script.ScriptEngine
import org.graalvm.polyglot.{Context, Engine, HostAccess}
import org.mojoz.querease.{ValidationException, ValidationResult, ValidationMessage}
import org.tresql.Query
import org.wabase.WabaseScriptValidation.{StringResultMessage, Validation, businessException, toScala, validationMessage}

import java.time.LocalDate
import java.util.Locale
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal


trait ScriptValidation {
  def validate(viewName: String, actionName: String, instance: Map[String, Any])(implicit locale: Locale): Unit
}

class WabaseScriptValidation(db: DbAccess, qe: AppQuerease)(implicit ec: ExecutionContext) extends ScriptValidation {

  private def invFun[T](fun: String, pars: Seq[(Class[_], () => Any)] = Nil,
                        ordPars: Seq[Any] = Nil) = {
    val ordParFun = ordPars.zipWithIndex
      .map { case (v, vIdx) => { case (_, pIdx) if pIdx == vIdx => v }: InvocationParameterFun }
      .foldLeft(PartialFunction.empty: InvocationParameterFun)(_ orElse _)
    invokeFunction(config.getString(fun),
      pars ++ Seq((classOf[DbAccess], () => db), (classOf[AppQuerease], () => qe)), ordParFun)
      .asInstanceOf[T]
  }

  private lazy val customFunctions: AnyRef = invFun[AnyRef]("app.script-validations.custom-functions-init")

  override def validate(viewName: String, actionName: String, instance: Map[String, Any])(
    implicit locale: Locale): Unit = {
    val validations = invFun[List[Validation]]("app.script-validations.load-validations",
      pars = Nil, ordPars = Seq(viewName, actionName)
    )
    if (validations.nonEmpty) {
      val engine = invFun[ScriptEngine]("app.script-validations.script-engine-init", Seq(
        (classOf[AnyRef], () => customFunctions), // put AnyRef in first position since it is superclass of remaining pars
        (classOf[Map[String, Any]], () => instance),
        (classOf[ScriptEngine], () => invFun[ScriptEngine]("app.script-validations.script-engine-factory")),
      ))
      def typeName(value: Any) = Option(value).map(_.getClass.getName).getOrElse("null")
      /* Invalid validation definition is a developer error, not a data validation failure,
       * so it is not reported as validation message */
      def definitionError(v: Validation, detail: String, cause: Throwable = null) =
        new RuntimeException(s"""Validation definition error (view $viewName, action $actionName, """ +
          s"""expression "${v.expression}", message "${v.message}"): $detail""", cause)
      def errorMsg(v: Validation): ValidationMessage = {
        //try to evaluate message as javascript, use original message if it is not valid javascript
        //message starting with brace is object literal, not block statement
        val js = if (v.message != null && v.message.trim.startsWith("{")) s"(${v.message})" else v.message
        val msg = try toScala(engine.eval(js)) catch { case NonFatal(_) => v.message }
        validationMessage(msg).getOrElse(throw definitionError(v, s"Wrong validation message type: ${typeName(msg)}"))
      }

      val validationResults = validations flatMap { v =>
        val result = try toScala(engine.eval(v.expression)) catch {
          case NonFatal(ex) =>
            // business exception from custom function is intended failure
            throw businessException(ex).getOrElse(definitionError(v, "Expression evaluation failed", ex))
        }
        val message = result match {
          case TRUE => null // OK
          case FALSE => errorMsg(v)
          case s: String =>
            val m = errorMsg(v)
            ValidationMessage(StringResultMessage, List(Map("msg" -> m.msg, "params" -> m.params), s))
          case x @ (_: List[_] | _: Map[_, _]) =>
            validationMessage(x).getOrElse(throw definitionError(v, s"Wrong validation result: $x"))
          case x => throw definitionError(v, s"Wrong validation result type: ${typeName(x)}")
        }
        Option(message).map(m => ValidationResult(Nil, List(m))).toList
      }
      if (validationResults.nonEmpty)
        throw new ValidationException(validationResults.flatMap(_.messages).map(_.msg).mkString("\n"), validationResults)
    }
  }
}

object WabaseScriptValidation {
  /** Error message template for validation expression returning string, parameters are validation message
    * as {msg, params} map and expression result */
  val StringResultMessage = """Error (validation "%1$s"): %2$s"""

  /** Converts polyglot values - live views of javascript arrays and objects - to plain scala values */
  def toScala(value: Any): Any = value match {
    case l: java.util.List[_]   => l.asScala.map(toScala).toList
    case m: java.util.Map[_, _] => m.asScala.map { case (k, v) => String.valueOf(k) -> toScala(v) }.toMap
    case v => v
  }

  /** Validation message from string - message template without parameters, list - message template
    * followed by parameters, or map with keys msg - message template and optional params - list of parameters.
    * Returns None for other values. */
  def validationMessage(value: Any): Option[ValidationMessage] = value match {
    case msg: String => Some(ValidationMessage(msg, Nil))
    case (msg: String) :: params => Some(ValidationMessage(msg, params))
    case m: Map[String @unchecked, _] if (m.keySet -- Set("msg", "params")).isEmpty =>
      (m.get("msg"), m.getOrElse("params", Nil)) match {
        case (Some(msg: String), params: List[_]) => Some(ValidationMessage(msg, params))
        case _ => None
      }
    case _ => None
  }

  /** Business exception in cause chain - thrown by custom function called from javascript */
  def businessException(ex: Throwable): Option[BusinessException] = ex match {
    case null => None
    case e: BusinessException => Some(e)
    case e => if (e.getCause eq e) None else businessException(e.getCause)
  }

  trait Validation {
    def context:    String
    def expression: String
    def message:    String
  }

  private class Validation_ extends org.wabase.Dto with Validation {
    var context:    String = null
    var expression: String = null
    var message:    String = null
  }

  def initWabaseScriptValidation(db: DbAccess, qe: AppQuerease)(implicit ec: ExecutionContext) =
    new WabaseScriptValidation(db, qe)

  def customFunctions(): AnyRef = CustomScriptValidationFunctions
  def scriptEngineFactory(): ScriptEngine = {
    GraalJSScriptEngine.create(
      Engine.newBuilder()
        .option("engine.WarnInterpreterOnly", "false")
        .build(),
      Context.newBuilder().allowExperimentalOptions(true)
        .allowHostAccess(HostAccess.EXPLICIT)
        .option("js.nashorn-compat", "true"),
    )
  }
  def initScriptEngine(instance: Map[String, Any],
                       engine: ScriptEngine, customFunctions: AnyRef): ScriptEngine = {
    val instancePropsToVars =
      instance
        .map {
          case (k, v) => s"var $k = ${ResultEncoder.encodeAnyToJsonString(v)};"
        }.mkString("\n")
    val functionDefs = customFunctions.getClass.getMethods
      .collect { case m if m.getAnnotation(classOf[Export]) != null =>
        (m.getName, ('a' to ('a'.toInt + m.getParameterTypes.length - 1).toChar)
          .mkString(", "))
      }
      .map { case (f, args) => s"$f = function($args) { return CustomFunctions.$f($args); };" }
      .mkString("\n")
    engine.put("CustomFunctions", customFunctions)
    engine.eval(functionDefs)
    engine.eval(instancePropsToVars)
    engine
  }
  def loadValidations(viewName: String, actionName: String, dbAccess: DbAccess)(
    implicit qe: AppQuerease): List[Validation] = {
    val validationsQuery =
      "validation[context ~~ :context] {context, expression, message}#(context)"
    dbAccess.withRollbackConn() { res =>
      Query(validationsQuery, Map("context" -> viewName))(res).map(r => new Validation_().fill(r)).toList
    }
  }
}

trait CustomScriptValidationFunctions {
  /** Current date as string in the format of date variables, so that they can be compared */
  @Export def current_date: String = Format.convertToString(java.sql.Date.valueOf(LocalDate.now()))
  /** Current time as string in the format of timestamp variables, so that they can be compared */
  @Export def now: String = Format.convertToString(new java.sql.Timestamp(currentTime))
  @Export def is_valid_email(email: String): Boolean =
    org.apache.commons.validator.routines.EmailValidator.getInstance.isValid(email)
}
object CustomScriptValidationFunctions extends CustomScriptValidationFunctions
