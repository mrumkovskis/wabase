package org.wabase

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.client.CoreClient

abstract class DataBaseSpecs[User] extends FlatSpec with Matchers with CoreClient with BeforeAndAfterAll with TemplateUtil {
  import AppMetadata._
  val ApplicationStateCookiePrefix = "current_"
  def defaultListParams: Map[String, Any] = Map("limit" -> 1)
  private var defaultListParamsForClass: Map[Class[_ <: Dto], Map[String, Any]] = Map()

  def listTest(clzz: Class[_ <: Dto], params: Map[String, Any]): Unit = defaultListParamsForClass += clzz -> params
  def listTest(clzz: Class[_ <: Dto], name: String, params: Map[String, Any]): Unit = createListTest(clzz, name, params)

  def views = qe.collectViews{ case v => v }.toSeq.sortBy(_.name)

  views.filter(_.apiMethodToRole.contains("list")).foreach{view =>
    val viewClass = qe.viewNameToClassMap(view.name)
    createListTest(viewClass, null, defaultListParamsForClass.getOrElse(viewClass, Map.empty))
  }

  override def beforeAll() = {
    login()
    listenToWs(deferredActor)
  }

  private def createListTest(viewClass: Class[_ <: Dto], name: String, params: => Map[String, Any]) = {
    it should "return list of "+viewClass+Option(name).map(" with "+_).getOrElse("") in {
      testList(viewClass, params)
    }

    it should "return count for list of "+viewClass+Option(name).map(" with "+_).getOrElse("") in {
      testCount(viewClass, params)
    }
  }

  def testList(viewClass: Class[_ <: Dto], params: => Map[String, Any]): Unit = {
    login()
    val (cookies, filteredParams) = params.partition(_._1.startsWith(ApplicationStateCookiePrefix))
    getCookieStorage.setCookies(cookies)
    list(viewClass, filteredParams ++ defaultListParams)
    clearCookies
  }

  def testCount(viewClass: Class[_ <: Dto], params: => Map[String, Any]): Unit = {
    login()
    val (cookies, filteredParams) = params.partition(_._1.startsWith(ApplicationStateCookiePrefix))
    getCookieStorage.setCookies(cookies)
    count(viewClass, filteredParams ++ defaultListParams)
    clearCookies
  }
}
