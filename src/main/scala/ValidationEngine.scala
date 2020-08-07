package org.wabase

import javax.script.ScriptEngineManager
import org.tresql.Query
import spray.json._

import scala.util.control.NonFatal

trait ValidationEngine {
  def validate(instance: Dto)
}

/** Default validation engine, executes validation javascript stored in "validation" table */
trait DefaultValidationEngine extends ValidationEngine with Loggable {
  this: QuereaseProvider with DbAccess =>

  import ValidationEngine._
  import qe.{viewDef, classToViewNameMap, MapJsonFormat}

  class Validation extends org.wabase.DtoWithId {
    var id: java.lang.Long = null
    var context: String = null
    var expression: String = null
    var message: String = null
  }

  val scriptEngineFactory = new ScriptEngineManager(null)
  def argsString(m: java.lang.reflect.Method) =
    ('a' to ('a'.toInt + m.getParameterTypes.size - 1).toChar)
       .mkString(", ")
   /** Custom functions available to validation scripts,
     * defaults to [[org.wabase.ValidationEngine.CustomValidationFunctions$]] */
  def customFunctions: AnyRef = CustomValidationFunctions
  lazy val customFunctionsAndArgs =
    customFunctions.getClass.getDeclaredMethods
      .map(m => (m.getName, argsString(m)))
      .toList
  lazy val createGlobalCustomFunctions =
    customFunctionsAndArgs
      .map { case (f, args) => s"$f = function($args) { return CustomFunctions.$f($args); };" }
      .mkString("\n")
  def get(instance: Dto) = {
    val engine = scriptEngineFactory getEngineByName "JavaScript"
    val instancePropsToVars =
      instance
        .toMap
        .toJson
        .asInstanceOf[JsObject]
        .fields
        .map {
          case (k, v) => s"var $k = $v;"
        }.mkString("\n")
    engine.put("CustomFunctions", customFunctions)
    engine.eval(createGlobalCustomFunctions)
    engine.eval(instancePropsToVars)
    engine
  }

  val validationsQuery =
    "validation[context ~~ :context] {id, context, expression, message}#(context, id)"

  def validations(instance: Dto): List[Validation] = {
    val viewName = viewDef(classToViewNameMap(instance.getClass)).name
    Query(validationsQuery, Map("context" -> viewName))(tresqlResources).map(r => new Validation().fill(r)).toList
  }
  override def validate(instance: Dto) {
    val validationList = validations(instance)
    if (validationList.nonEmpty) {
      val engine = get(instance)

      def errorMsg(msg: String) = if (msg.startsWith("\"") || msg.startsWith("'")) {
        //dynamic error messages if message starts with single or double quote
        try { String.valueOf(engine.eval(msg)) } catch {
          case NonFatal(e) =>
            val emsg = s"Error evaluating validation error msg - $msg"
            logger.error(emsg, e)
            emsg
          case x => throw x
      }
      } else msg

      validationList foreach { v =>
        val result = try engine.eval(v.expression) catch {
          case ex: Exception =>
            val msg =
              (("Validation error \"" + errorMsg(v.message) + "\"") :: Format.msgList(ex))
                .mkString("\n  caused by: ")
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

trait NoValidation extends ValidationEngine {
  override def validate(instance: Dto) {}
}

object ValidationEngine {
  trait CustomValidationFunctions {
   def current_date = {
     import java.util.Calendar
     val d = new java.util.Date
     val cal = Calendar.getInstance
     cal setTime d
     cal.set(Calendar.HOUR_OF_DAY, 0)
     cal.set(Calendar.MINUTE, 0)
     cal.set(Calendar.SECOND, 0)
     cal.set(Calendar.MILLISECOND, 0)
     new java.sql.Date(cal.getTime.getTime)
   }
   def now = new java.sql.Timestamp(currentTime)
   def is_valid_email(email: String): Boolean =
     org.apache.commons.validator.routines.EmailValidator.getInstance.isValid(email)
  }
  object CustomValidationFunctions extends CustomValidationFunctions
}
