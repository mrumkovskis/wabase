package dto

import org.wabase.Dto

object DtoMapping {
  val viewNameToClass: Map[String, Class[_ <: Dto]] = Map(
    "request_calculation_view" -> classOf[request_calculation_view],
    "guideline_calculation_helper" -> classOf[guideline_calculation_helper],
    "response_calculation_view" -> classOf[response_calculation_view],
  )
}

class request_calculation_view extends Dto {
  var category: String = null
}

class guideline_calculation_helper extends Dto {
  var code: String = null
  var category: String = null
  var description: String = null
}

class response_calculation_view extends Dto {
  var code: String = null
  var category: String = null
  var description: String = null
}
