package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.PropertyResourceBundle
import scala.jdk.CollectionConverters._

class WabaseBundleSpecs extends FlatSpec with Matchers {

  /** Keys of single resource file, without parent bundle fallback */
  def bundleKeys(language: String): Set[String] = {
    val name = s"/wabase_$language.properties"
    val in = getClass.getResourceAsStream(name)
    assert(in != null, s"Resource $name not found")
    try new PropertyResourceBundle(new InputStreamReader(in, StandardCharsets.UTF_8)).keySet.asScala.toSet
    finally in.close()
  }

  val languages = List("en", "lv")

  /** User facing message templates which must be translated */
  val userFacingMessages: List[String] = {
    import AppBase._
    import Authentication._
    import DbConstraintMessage.PostgreSqlConstraintMessageBuilder.{Ck, FkDel, FkIns, Nn, Uk}
    List(
      RecordNotFoundCannotEditMessage,
      FieldRequiredMessage,
      FieldValueTooLongMessage,
      FieldValueNotInEnumMessage,
      BadEmailAddressMessage,
      WrongPasswordOrUsernameMessage,
      AuthenticationFailedMessage,
      UnexpectedAuthenticationErrorMessage,
      AppFileStreamer.CannotProcessFileMessage,
      AppServiceBase.AppExceptionHandler.PostgresTimeoutExceptionHandler.TimeoutFriendlyMessage,
    ) ++ List(Nn, FkDel, FkIns, Uk, Ck).map(_.genericMessage)
  }

  behavior of "wabase resource bundles"

  it should "contain the same keys for all languages" in {
    val keys = languages.map(l => l -> bundleKeys(l)).toMap
    for (l1 <- languages; l2 <- languages if l1 != l2)
      withClue(s"Keys in wabase_$l1 missing in wabase_$l2: ") {
        (keys(l1) -- keys(l2)) shouldBe empty
      }
  }

  it should "contain user facing messages" in {
    for (l <- languages) {
      val keys = bundleKeys(l)
      withClue(s"Messages missing in wabase_$l: ") {
        userFacingMessages.filterNot(keys.contains) shouldBe empty
      }
    }
  }
}
