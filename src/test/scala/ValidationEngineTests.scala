package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

class ValidationEngineTestDto extends Dto {
  var expression: String = null
  var message: String = null
  var my_string_field: String = "mystring"
  var my_int_field: java.lang.Integer = 42
  var my_bool_field: java.lang.Boolean = true
}

object TestValidationEngine extends org.wabase.TestApp {
  override def validations(instance: org.wabase.Dto) =  {
    val v = new Validation
    val test = instance.asInstanceOf[ValidationEngineTestDto]
    v.expression = test.expression
    v.message = test.message
    List(v)
  }
}

class ValidationEngineTests extends FlatSpec with Matchers {

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
    intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto(
        "false",
        "false throws"
      ))
    }.getMessage should be ("false throws")
    TestValidationEngine.validate(validationTestDto(
      "1 + 1 === 2",
      "true ok"
    ))
    intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto(
        "1 + 1 === 5",
        "false throws"
      ))
    }.getMessage should be ("false throws")
  }

  "validation engine" should "support variables" in {
    intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto(
        "my_string_field",
        "string throws"
      ))
    }.getMessage should be ("""Error (validation "string throws"): mystring""")
    intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto(
        "my_string_field + ', ' + my_int_field",
        "dynamic message"
      ))
    }.getMessage should be ("""Error (validation "dynamic message"): mystring, 42""")
    intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto(
        "my_int_field === 43",
        "false throws"
      ))
    }.getMessage should be ("false throws")
    TestValidationEngine.validate(validationTestDto(
      "my_bool_field",
      "true ok"
    ))
    intercept[BusinessException] {
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
    intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto(
        "is_valid_email(my_string_field)",
        "invalid email"
      ))
    }.getMessage should be ("invalid email")
  }

  "validation engine" should "support dynamic error messages" in {
    intercept[BusinessException] {
      TestValidationEngine.validate(validationTestDto(
        "my_int_field === 43",
        "'Should be 43, found - ' + my_int_field"
      ))
    }.getMessage should be ("Should be 43, found - 42")
  }
}
