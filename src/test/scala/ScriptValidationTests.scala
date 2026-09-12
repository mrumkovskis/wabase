package org.wabase

import org.graalvm.polyglot.HostAccess.Export
import org.mojoz.querease.{ValidationException, ValidationMessage, ValidationResult}

import java.util.Locale
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.AppMetadata.Action
import org.wabase.WabaseScriptValidation.Validation

import scala.concurrent.ExecutionContext

class ValidationEngineTestDto extends Dto {
  var expression: String = null
  var message: String = null
  var my_string_field: String = "mystring"
  var my_int_field: java.lang.Integer = 42
  var my_bool_field: java.lang.Boolean = true
  var my_date_field: java.sql.Date = java.sql.Date.valueOf("2020-01-01")
  var my_timestamp_field: java.sql.Timestamp = java.sql.Timestamp.valueOf("2020-01-01 10:20:30")
}

object TestScriptValidationFunctions extends CustomScriptValidationFunctions {
  @Export def business_error(msg: String): Boolean = throw new BusinessException(msg)
}

object TestValidationEngine extends org.wabase.TestApp {
  val validationModule = new WabaseScriptValidation(this, qe)(ExecutionContext.global)
  def customFunctions(): AnyRef = TestScriptValidationFunctions
  private val threadLocalValidations = new ThreadLocal[List[Validation]]
  override protected def isScriptValidationEnabled: Boolean = true
  def loadValidations(viewName: String, actionName: String) = threadLocalValidations.get
  def validations(instance: org.wabase.Dto) =  {
    val test = instance.asInstanceOf[ValidationEngineTestDto]
    val v = new Validation {
      var context:    String = null
      var expression: String = test.expression
      var message:    String = test.message
    }
    List(v)
  }
  def validate(instance: org.wabase.Dto)(implicit locale: Locale): Unit = {
    threadLocalValidations.set(validations(instance))
    validationModule.validate("fake-view", Action.Save, instance.toMap(qe))
  }
}

class ValidationEngineTests extends FlatSpec with Matchers {
  implicit val locale: Locale = Locale.getDefault()

  def vm(msg: String, params: Any*) = ValidationMessage(msg, params.toList)
  def failures(expression: String, message: String): List[ValidationMessage] =
    intercept[ValidationException] {
      TestValidationEngine.validate(validationTestDto(expression, message))
    }.details.flatMap { case ValidationResult(Nil, messages) => messages }
  def definitionError(expression: String, message: String): RuntimeException = {
    val ex = intercept[RuntimeException] {
      TestValidationEngine.validate(validationTestDto(expression, message))
    }
    ex.getClass shouldBe classOf[RuntimeException] // not BusinessException or other subclass
    ex.getMessage should startWith (s"""Validation definition error (view fake-view, action save, expression "$expression", message "$message"): """)
    ex
  }
  def stringResult(message: String, messageParams: List[Any], result: String) =
    vm("""Error (validation "%1$s"): %2$s""", Map("msg" -> message, "params" -> messageParams), result)

  def validationTestDto(expression: String, message: String) = {
    val t = new ValidationEngineTestDto
    t.expression = expression
    t.message = message
    t
  }

  "validation engine" should "validate javascript expressions" in {
    TestValidationEngine.validate(validationTestDto(
      "true",
      "true ok"
    ))
    intercept[ValidationException] {
      TestValidationEngine.validate(validationTestDto(
        "false",
        "false throws"
      ))
    }.getMessage should be ("false throws")
    TestValidationEngine.validate(validationTestDto(
      "1 + 1 === 2",
      "true ok"
    ))
    intercept[ValidationException] {
      TestValidationEngine.validate(validationTestDto(
        "1 + 1 === 5",
        "false throws"
      ))
    }.getMessage should be ("false throws")
  }

  "validation engine" should "support variables" in {
    failures("my_string_field", "string throws") shouldBe
      List(stringResult("string throws", Nil, "mystring"))
    failures("my_string_field + ', ' + my_int_field", "dynamic message") shouldBe
      List(stringResult("dynamic message", Nil, "mystring, 42"))
    intercept[ValidationException] {
      TestValidationEngine.validate(validationTestDto(
        "my_int_field === 43",
        "false throws"
      ))
    }.getMessage should be ("false throws")
    TestValidationEngine.validate(validationTestDto(
      "my_bool_field",
      "true ok"
    ))
    intercept[ValidationException] {
      TestValidationEngine.validate(validationTestDto(
        "!my_bool_field",
        "false throws"
      ))
    }.getMessage should be ("false throws")
  }

  "validation engine" should "support custom functions" in {
    TestValidationEngine.validate(validationTestDto(
      "is_valid_email('e@mail.com')",
      "email ok"
    ))
    intercept[ValidationException] {
      TestValidationEngine.validate(validationTestDto(
        "is_valid_email(my_string_field)",
        "invalid email"
      ))
    }.getMessage should be ("invalid email")
  }

  "validation engine" should "support current_date and now functions comparable with date variables" in {
    val today = Format.convertToString(java.sql.Date.valueOf(java.time.LocalDate.now()))
    def passes(expression: String) = TestValidationEngine.validate(validationTestDto(expression, s"failed: $expression"))
    passes("typeof my_date_field === 'string' && typeof my_timestamp_field === 'string'")
    passes(s"current_date() === '$today'")
    passes("typeof now() === 'string' && now().substring(0, 10) === current_date()")
    passes("current_date() > my_date_field && my_date_field < current_date()")
    passes("now() > my_timestamp_field && my_timestamp_field < now()")
    passes("current_date() > '2020-01-01' && current_date() < '2100-01-01'")
    failures("my_date_field >= current_date()", "['Date %1$s must not be before %2$s', my_date_field, current_date()]") shouldBe
      List(vm("Date %1$s must not be before %2$s", "2020-01-01", today))
  }

  "validation engine" should "support dynamic error messages" in {
    intercept[ValidationException] {
      TestValidationEngine.validate(validationTestDto(
        "my_int_field === 43",
        "'Should be 43, found - ' + my_int_field"
      ))
    }.getMessage should be ("Should be 43, found - 42")
  }

  "validation engine" should "support error message parameters" in {
    // message evaluated to array - template followed by parameters
    failures("my_int_field === 43", "['Should be %1$s, found %2$s', 43, my_int_field]") shouldBe
      List(vm("Should be %1$s, found %2$s", 43, 42))
    // message evaluated to object, with or without parameters
    failures("my_int_field === 43", "{msg: 'Should be %1$s, found %2$s', params: [43, my_int_field]}") shouldBe
      List(vm("Should be %1$s, found %2$s", 43, 42))
    failures("my_int_field === 43", "({msg: 'No parameters'})") shouldBe
      List(vm("No parameters"))
    // template which is not valid javascript is used as is
    failures("my_int_field === 43", "Field %1$s is mandatory") shouldBe
      List(vm("Field %1$s is mandatory"))
    // nested parameter values
    failures("my_int_field === 43", "['Values %1$s', [1, 'two'], {a: null}]") shouldBe
      List(vm("Values %1$s", List(1, "two"), Map("a" -> null)))
    // expression evaluated to array or object is the message itself
    failures("my_int_field === 43 || ['Should be %1$s, found %2$s', 43, my_int_field]", "not used") shouldBe
      List(vm("Should be %1$s, found %2$s", 43, 42))
    failures("my_int_field === 43 || {msg: 'Should be %1$s', params: [43]}", "not used") shouldBe
      List(vm("Should be %1$s", 43))
    // expression evaluated to string - message with parameters is nested
    failures("my_string_field", "['Field %1$s', 'my_string_field']") shouldBe
      List(stringResult("Field %1$s", List("my_string_field"), "mystring"))
    TestValidationEngine.validate(validationTestDto(
      "my_int_field === 42 || ['Should be %1$s, found %2$s', 42, my_int_field]",
      "not used"
    ))
  }

  "validation engine" should "treat wrong validation definition as developer error" in {
    // wrong expression result type
    definitionError("my_string_field.missing", "message").getMessage should endWith ("Wrong validation result type: null")
    definitionError("undefined", "message").getMessage should endWith ("Wrong validation result type: null")
    definitionError("my_int_field", "message").getMessage should endWith ("Wrong validation result type: java.lang.Integer")
    definitionError("[]", "message").getMessage should endWith ("Wrong validation result: List()")
    definitionError("[42, 'x']", "message").getMessage should endWith ("Wrong validation result: List(42, x)")
    // expression evaluation failure
    val ex = definitionError("no_such_var", "message")
    ex.getMessage should endWith ("Expression evaluation failed")
    ex.getCause should not be null
    // wrong message type
    definitionError("false", "42").getMessage should endWith ("Wrong validation message type: java.lang.Integer")
    definitionError("false", "undefined").getMessage should endWith ("Wrong validation message type: null")
    definitionError("false", "[42, 'x']").getMessage should endWith ("Wrong validation message type: scala.collection.immutable.$colon$colon")
    definitionError("false", "{params: [1]}").getMessage should include ("Wrong validation message type")
    definitionError("false", "{msg: 'x', parms: [1]}").getMessage should include ("Wrong validation message type")
    definitionError("false", "{msg: 'x', params: 1}").getMessage should include ("Wrong validation message type")
  }

  "validation engine" should "propagate business exception from custom function" in {
    val ex = intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto("business_error('not allowed')", "message"))
    }
    ex.getMessage shouldBe "not allowed"
  }
}
