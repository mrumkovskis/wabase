package org.wabase

import com.oracle.truffle.js.scriptengine.GraalJSScriptEngine
import com.typesafe.scalalogging.Logger
import org.graalvm.polyglot.HostAccess.Export

import javax.script.ScriptEngine
import org.graalvm.polyglot.{Context, Engine, HostAccess}
import org.slf4j.LoggerFactory
import org.tresql.Query
import org.wabase.WabaseScriptValidation.Validation

import java.time.LocalDate
import java.util.Locale
import scala.concurrent.ExecutionContext
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
      def errorMsg(msg: String) =
        //try to evaluate message as javascript
        try { String.valueOf(engine.eval(msg)) } catch {
          case NonFatal(_) => msg //return original message
        }

      validations foreach { v =>
        val result = try engine.eval(v.expression) catch {
          case ex: Exception =>
            val msg =
              (("Validation error \"" + errorMsg(v.message) + "\"") :: Format.msgList(ex))
                .mkString("\n  caused by: ")
            val logger = Logger(LoggerFactory.getLogger(s"$viewName.$actionName"))
            logger.debug(msg)
            throw new BusinessException(msg)
        }
        result match {
          case TRUE => // OK
          case FALSE =>
            throw new BusinessException(errorMsg(v.message))
          case s: String =>
            throw new BusinessException(
              s"""Error (validation "${errorMsg(v.message)}"): $s""")
          case x =>
            throw new BusinessException(
              "Validation error \"" + errorMsg(v.message) + "\": " +
                "Wrong validation result type: " +
                Option(x).map(_.getClass.getName).getOrElse(x))
        }
      }
    }
  }
}

object WabaseScriptValidation {
  class Validation extends org.wabase.DtoWithId {
    var id: java.lang.Long = null
    var context: String = null
    var expression: String = null
    var message: String = null
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
      "validation[context ~~ :context] {id, context, expression, message}#(context, id)"
    dbAccess.withRollbackConn() { res =>
      Query(validationsQuery, Map("context" -> viewName))(res).map(r => new Validation().fill(r)).toList
    }
  }
}

trait CustomScriptValidationFunctions {
  @Export def current_date = java.sql.Date.valueOf(LocalDate.now())
  @Export def now = new java.sql.Timestamp(currentTime)
  @Export def is_valid_email(email: String): Boolean =
    org.apache.commons.validator.routines.EmailValidator.getInstance.isValid(email)
}
object CustomScriptValidationFunctions extends CustomScriptValidationFunctions
