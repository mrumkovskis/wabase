package org.wabase.swagger

import io.swagger.v3.core.util.{Json, Json31, Yaml, Yaml31}
import io.swagger.v3.oas.models._
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.media._
import io.swagger.v3.oas.models.parameters.{PathParameter, QueryParameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.security.{SecurityRequirement, SecurityScheme}
import io.swagger.v3.oas.models.servers.Server
import org.apache.commons.lang3.StringUtils
import org.mojoz.metadata.{FieldDef, ViewDef}
import org.mojoz.querease.FilterType.{ComparisonFilter, OtherFilter}
import org.mojoz.querease.Querease
import org.wabase.AppMetadata.Action.{Evaluation, Validations, ViewCall}
import org.wabase.AppMetadata.{AugmentedAppFieldDef, AugmentedAppViewDef, FilterParameter, RouteDef}
import org.wabase.{AppQuerease, Loggable}

import java.net.URI
import scala.collection.immutable.{Map, TreeMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.language.existentials
import scala.util.control.NonFatal

class WabaseSwaggerGenerator(
  qes: Seq[Querease],
  hostString: String,
  isRelevantView:     ViewDef  => Boolean = _.apiMethodToRoles.nonEmpty,
  isRelevantRoute:    RouteDef => Boolean = _ => true,
) extends Loggable {
  private val hostUri = new URI(hostString.stripSuffix("/"))
  def host: String = hostUri.getHost +
    (if (hostUri.getPort == -1) ""
     else ":" + hostUri.getPort) //the url of your api, not swagger's json endpoint
  def schemes: List[String] = List(hostUri.getScheme) //the url of your api, not swagger's json endpoint
  def basePath: String = ""
  def info: Info = new Info()
      .description("")
      .version("")
      .title("")
      .termsOfService("")
      // .contact(null)
      // .license(null)
      // .addExtension(k, v)
  def components: Option[Components] = None
  def security: List[SecurityRequirement] = List.empty
  def securitySchemes: Map[String, SecurityScheme] = Map.empty
  def externalDocs: Option[ExternalDocumentation] = None
  def vendorExtensions: Map[String, Object] = Map.empty
  def specVersion: SpecVersion = SpecVersion.V31

  def swaggerConfig: OpenAPI = {
    val swagger = new OpenAPI()
    val sv = specVersion
    swagger.setSpecVersion(sv)
    val version = if (sv == SpecVersion.V31) "3.1.0" else "3.0.1"
    swagger.setOpenapi(version)
    swagger.setInfo(info)
    components.foreach { c => swagger.setComponents(c) }
    val path = basePath.stripPrefix("/")
    val hostPath =
      if (StringUtils.isNotBlank(path))
          s"${host.stripSuffix("/")}/${path}/"
      else host
    schemes.foreach { scheme =>
      swagger.addServersItem(new Server().url(s"${scheme.toLowerCase}://$hostPath"))
    }
    if (schemes.isEmpty && StringUtils.isNotBlank(hostPath)) {
      swagger.addServersItem(new Server().url(hostPath))
    }
    securitySchemes.foreach { case (k: String, v: SecurityScheme) => swagger.schemaRequirement(k, v) }
    swagger.setSecurity((mutable.ListBuffer.empty ++ security).asJava)
    swagger.extensions((mutable.Map.empty ++ vendorExtensions).asJava)
    externalDocs.foreach { ed => swagger.setExternalDocs(ed) }
    swagger
  }

  lazy val viewNameToQe = qes.flatMap { qe =>
    qe.nameToViewDef.map { case (n, v) => (n, qe) }
  }.toMap

  lazy val viewdefs =
    qes.flatMap { qe =>
      dropIrrelevant(qe, qe.nameToViewDef.values.toList)
    }
  lazy val viewDefMap = viewdefs.map(v => v.name -> v).toMap

  // https://swagger.io/docs/specification/data-models/data-types/
  def schemaFromType(type_ : org.mojoz.metadata.Type) = type_.name match {
    case n if type_.isComplexType =>
      (new Schema).$ref(refFromViewName(n))
    case "long" => (new IntegerSchema).format("int64")
    case "int" => new IntegerSchema
    case "decimal" => new NumberSchema
    case "boolean" => new BooleanSchema
    case "date" => new DateSchema
    case "dateTime" => new DateTimeSchema
    case "timestamp" => new DateTimeSchema
    case "timeuuid" => new StringSchema
    case "string" =>
      val s = new StringSchema
      type_.length.foreach(l => s.maxLength(l))
      s
    case "json" => new ObjectSchema
    case "yaml" => new StringSchema
    case n if n.endsWith("String") => new StringSchema
    case n => (new ObjectSchema).`type`(n)
  }

  def getReadOnly(viewdefs: Map[String, ViewDef])(field: FieldDef) = {
    if (!field.api.updatable && !field.api.insertable) true
    else if (field.type_.isComplexType) viewdefs.get(field.type_.name).exists(_.saveTo == Nil)
    else false
  }

  def fieldRequired(viewdefs: Map[String, ViewDef])(field: FieldDef) =
    !getReadOnly(viewdefs)(field) && (field.required || !field.nullable)

  def addEnumIfNeeded(enums: Seq[String], schema: Schema[_]) = {
    (enums, schema) match {
      case (enums, stringSchema: StringSchema) if enums != null => enums.foreach(stringSchema.addEnumItem)
      case _ =>
    }
    schema
  }

  def schemaFromFieldDef(viewdefs: Map[String, ViewDef])(field: FieldDef): (String, Schema[_]) = {
    val maybeArraySchema = if (field.isCollection) {
      new ArraySchema().items(schemaFromType(field.type_))
    } else schemaFromType(field.type_)
    addEnumIfNeeded(field.enum_, maybeArraySchema)
    val fieldName = field.fieldName
    fieldName -> maybeArraySchema
      .name(fieldName)
      .readOnly(getReadOnly(viewdefs)(field))
      .description(Option(field.comments).getOrElse(field.label))
  }

  def isApiField(f: FieldDef): Boolean =
    !f.api.excluded

  def shouldIncludeSchemaForView(v: ViewDef): Boolean = true
  def schemasFromViewDefs: Map[String, Schema[_]] = {
    viewDefMap.view.filter { case (_, v) => shouldIncludeSchemaForView(v) }.map { case (key, viewDef) =>
      val filteredFields = viewDef.fields.filter(isApiField)
      val fields: TreeMap[String, Schema[_]] =
        TreeMap()(viewNameToQe(viewDef.name).fieldOrdering(viewDef.name)) ++
        filteredFields.map(schemaFromFieldDef(viewDefMap)).toMap
      val fieldsAsJava = new java.util.LinkedHashMap[String, Schema[_]](fields.size, 1)
      fields.foreach { case (name, schema) => fieldsAsJava.put(name, schema) }
      val requiredFields = filteredFields.filter(fieldRequired(viewDefMap)).map(_.fieldName).toList
      key -> new ObjectSchema()
        .name{viewDef.name}
        .description(viewDef.comments)
        .properties(fieldsAsJava)
        .required(requiredFields.asJava): (String, Schema[_]) /* cast for scala 2.12 */
    }.toMap - "count" // no object schema for "count" service
  }

  def refFromViewName(viewName: String) = s"#/components/schemas/$viewName"

  def fileContent(view: String): Content = {
    val content = new Content
    val mediaType = new MediaType
    content.addMediaType("application/octet-stream", mediaType)
  }

  lazy val dataDownloadContentTypes: Seq[String] = Vector(
    "application/json",
    "text/csv",
  )
  def dataDownloadContent(view: String): Content = {
    val content = new Content
    val mediaType = new MediaType
    mediaType.setSchema(new ArraySchema().items(new ObjectSchema))
    dataDownloadContentTypes.foreach(content.addMediaType(_, mediaType))
    content
  }

  def fileUploadContent(view: String): Content = {
    val content = new Content
    val mediaType = new MediaType
    val schema = new FileSchema
    mediaType.setSchema(schema)
    content.addMediaType("application/octet-stream", mediaType)
  }

  def jsonContent(view: String, array: Boolean = false): Content = {
    val content = new Content
    val mediaType = new MediaType
    val viewSchema = view match {
      case "count"  => schemaFromType(new org.mojoz.metadata.Type("long"))
      case _        => (new Schema()).$ref(refFromViewName(view))
    }
    val maybeArraySchema = if (array) {
      new ArraySchema().items(viewSchema)
    } else viewSchema

    mediaType.setSchema(maybeArraySchema)
    content.addMediaType("application/json", mediaType)
    content
  }

  def plaintextContent(view: String): Content = {
    val content = new Content
    val mediaType = new MediaType
    mediaType.setSchema(new StringSchema)
    content.addMediaType("text/plain", mediaType)
  }

  def responseContent(view: String, array: Boolean = false) =
    jsonContent(view, array)

  def createOperation(summary: String, description: String): Operation =
    (new Operation).summary(summary).description(description)

  def createOperation(method: String, viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation(summary(method, viewDef, keySize), description(method, viewDef, keySize))

  def addPathParameter(op: Operation, field: FieldDef): Operation = {
    val p = new PathParameter
    p.name(field.fieldName)
    p.setDescription(Option(field.comments).orElse(Option(field.label)).getOrElse(field.fieldName))
    p.setSchema(addEnumIfNeeded(field.enum_, schemaFromType(field.type_)))
    op.addParametersItem(p)
    op
  }

  def addPathParameters(op: Operation, method: String, viewDef: ViewDef, keySize: Int = 99): Operation = {
    viewDef.keyFieldNames.take(keySize).foreach { keyFieldName =>
      val field = viewDef.fieldOpt(keyFieldName).getOrElse(
        new org.mojoz.metadata.FieldDef(keyFieldName, new org.mojoz.metadata.Type("string")))
      op.addParametersItem {
        val p = new PathParameter
        p.name(keyFieldName)
        p.setDescription(Option(field.comments).orElse(Option(field.label)).getOrElse(keyFieldName))
        p.setSchema(addEnumIfNeeded(field.enum_, schemaFromType(field.type_)))
        p
      }
    }
    op
  }

  def getQueryParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Seq[FilterParameter] = {
    if (method == "list") {
      viewNameToQe(viewDef.name) match {
        case qe: AppQuerease => qe.filterParameters(viewDef)
        case _ => Nil
      }
    } else Nil
  }

  def addQueryParameter(op: Operation, param: FilterParameter): Operation = {
    val p = new QueryParameter
    p.name(param.name)
    val required = param.filterType match {
      case OtherFilter(filter) if filter.startsWith("if_defined") => false
      case _ => param.required
    }
    val schema = param.filterType match {
      case ComparisonFilter(_, "in", _, _) => new ArraySchema().items(schemaFromType(param.type_))
      // TODO should have better filter processing.
      //   This one checks for complex exists queries. if there is "in" statement with given param
      case OtherFilter(text) if text.contains(s" in :${param.name}") =>
        new ArraySchema().items(schemaFromType(param.type_))
      case _ => schemaFromType(param.type_)
    }

    p.required(required)
    p.setSchema(addEnumIfNeeded(param.enum_, schema))
    op.addParametersItem(p)
    op
  }

  def addQueryParameters(op: Operation, method: String, viewDef: ViewDef, keySize: Int = 99): Operation = {
    val params = getQueryParameters(method, viewDef, keySize)
    if (params != null)
      params.foreach(addQueryParameter(op, _))
    op
  }

  def addHeaderParameters(op: Operation, method: String, viewDef: ViewDef, keySize: Int = 99): Operation = {
    op
  }

  def addCookieParameters(op: Operation, method: String, viewDef: ViewDef, keySize: Int = 99): Operation = {
    op
  }

  def addParameters(op: Operation, method: String, viewDef: ViewDef, keySize: Int = 99): Operation = {
    addPathParameters  (op, method, viewDef, keySize)
    addQueryParameters (op, method, viewDef, keySize)
    addHeaderParameters(op, method, viewDef, keySize)
    addCookieParameters(op, method, viewDef, keySize)
  }

  def getResponses(op: Operation): ApiResponses =
    if (op.getResponses == null) {
      val r = new ApiResponses
      op.setResponses(r)
      r
    } else op.getResponses

  def addIntegerResponse(op: Operation, description: String, code: String = "200"): Operation = {
    val responses = getResponses(op)
    val response = new ApiResponse
    response.description(description)

    val content = new Content
    val mediaType = new MediaType
    mediaType.setSchema(new IntegerSchema)
    content.addMediaType("text/plain", mediaType)
    response.content(content)
    responses.addApiResponse(code, response)
    op
  }

  def addSuccessResponse(op: Operation, view: String, code: String = "200", array: Boolean = false): Operation = {
    val responses = getResponses(op)
    val response = new ApiResponse
    response.description("")
    if (hasApiFields(view))
      response.content(responseContent(view, array))
    responses.addApiResponse(code, response)
    op
  }

  def addSuccessPlaintextResponse(op: Operation, view: String, code: String = "200"): Operation = {
    val responses = getResponses(op)
    val response = new ApiResponse
    response.description("")
    response.content(plaintextContent(view))
    responses.addApiResponse(code, response)
    op
  }

  def addErrorResponse(op: Operation, code: String, description: String, content: Content = null): Operation = {
    val responses = getResponses(op)
    val response = new ApiResponse
    response.description(description)
    if (content != null) response.content(content)
    responses.addApiResponse(code, response)
    op
  }

  def addBadRequestResponse(op: Operation)     = addErrorResponse(op, "400", "Bad request")
  def addForbiddenResponse(op: Operation, viewDef: ViewDef) = addErrorResponse(op, "403", "Forbidden")
  def addNotFoundResponse(op: Operation)       = addErrorResponse(op, "404", "Not Found")
  def addInternalServerError(op: Operation)    = addErrorResponse(op, "500", "Internal server error")
  def addServiceUnavailabeError(op: Operation) = addErrorResponse(op, "503", "Service Unavailable")

  def hasApiFields(view: String) =
    viewDefMap.get(view).exists(_.fields.exists(isApiField))

  def addRequestBody(op: Operation, view: String, array: Boolean = false): Operation = {
    if (hasApiFields(view)) {
      val body = new RequestBody()
      body.setContent(responseContent(view, array))
      op.setRequestBody(body)
    }
    op
  }

  def addFileRequestBody(op: Operation, view: String): Operation = {
    val body = new RequestBody()
    body.setContent(fileUploadContent(view))
    op.setRequestBody(body)
    op
  }

  implicit class RichOperation(val op: Operation) {
    val delegate = WabaseSwaggerGenerator.this
    def addBadRequestResponse = delegate.addBadRequestResponse(op)
    def addCookieParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Operation =
          delegate.addCookieParameters(op, method, viewDef, keySize)
    def addErrorResponse(code: String, description: String, content: Content = null): Operation =
          delegate.addErrorResponse(op, code, description, content)
    def addFileRequestBody(view: String): Operation = delegate.addFileRequestBody(op, view)
    def addForbiddenResponse(viewDef: ViewDef) = delegate.addForbiddenResponse(op, viewDef)
    def addHeaderParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Operation =
          delegate.addHeaderParameters(op, method, viewDef, keySize)
    def addIntegerResponse(description: String, code: String = "200"): Operation =
          delegate.addIntegerResponse(op, description, code)
    def addInternalServerError = delegate.addInternalServerError(op)
    def addNotFoundResponse  = delegate.addNotFoundResponse(op)
    def addParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Operation =
          delegate.addParameters(op, method, viewDef, keySize)
    def addPathParameter(field: FieldDef): Operation =
          delegate.addPathParameter(op, field)
    def addPathParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Operation =
          delegate.addPathParameters(op, method, viewDef, keySize)
    def addQueryParameter(param: FilterParameter): Operation =
          delegate.addQueryParameter(op, param)
    def addQueryParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Operation =
          delegate.addQueryParameters(op, method, viewDef, keySize)
    def addRequestBody(view: String, array: Boolean = false): Operation = delegate.addRequestBody(op, view, array)
    def addServiceUnavailabeError = delegate.addServiceUnavailabeError(op)
    def addSuccessPlaintextResponse(view: String, code: String = "200"): Operation =
          delegate.addSuccessPlaintextResponse(op, view, code)
    def addSuccessResponse(view: String, code: String = "200", array: Boolean = false): Operation =
          delegate.addSuccessResponse(op, view, code, array)
    def getResponses: ApiResponses = delegate.getResponses(op)
  }

  def getErrorCodesForView(viewDef: ViewDef, action: String): List[String] = {
    val steps = viewDef.actions.get(action).map(_.steps).getOrElse(Nil)
    val res = steps.flatMap {
      case (Validations(Some(name), _, _), _) => List(name)
      case (Evaluation(_, _, ViewCall(method, view, data), _), _) if view != viewDef.name =>
        viewNameToQe(view).nameToViewDef.get(view).map(subView =>
          getErrorCodesForView(subView, method)
        ).getOrElse(Nil)
      case _ => Nil
    }
    res
  }

  def keyDescription(viewDef: ViewDef, keySize: Int = 99): String = {
    val keyFieldNames = viewDef.keyFieldNames.take(keySize)
    keyFieldNames.size match {
      case 0 => ""
      case 1 => s"""by '${keyFieldNames.head}'"""
      case x => s"""by '${keyFieldNames.mkString("' and '")}'"""
    }
  }

  def summary(method: String, viewDef: ViewDef, keySize: Int = 99): String = {
    val viewName = viewDef.name
    val methodTitleCase = s"${Character.toTitleCase(method.charAt(0))}${method.substring(1)}"
    s"$methodTitleCase '$viewName' ${keyDescription(viewDef, keySize)}".trim
  }

  def description(method: String, viewDef: ViewDef, keySize: Int = 99): String = {
    method match {
      case "delete" => ""
      case _        => Option(viewDef.comments).getOrElse("")
    }
  }

  def rootPathForView(viewDef: ViewDef) = s"/${viewDef.name}"

  def pathWithKey(method: String, viewDef: ViewDef, keySize: Int = 99) = {
    val infix = method match {
      case "create" => s":$method"
      case "count"  => s":$method"
      case _        =>  ""
    }
    if (viewDef.keyFieldNames.take(keySize).isEmpty)
      s"${rootPathForView(viewDef)}$infix"
    else
      s"${rootPathForView(viewDef)}$infix/${viewDef.keyFieldNames.take(keySize).mkString("{", "}/{", "}")}"
  }

  def isArrayRequest(viewDef: ViewDef, method: String) = false

  def operationForCreate(viewDef: ViewDef): Operation =
    createOperation("create", viewDef)
      .addParameters("create", viewDef)
      .addSuccessResponse(view = viewDef.name)
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addNotFoundResponse
      .addInternalServerError
      .addServiceUnavailabeError

  def operationForCount(viewDef: ViewDef): Operation =
    createOperation("count", viewDef)
      .addParameters("count", viewDef)
      .addIntegerResponse("Count")
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addNotFoundResponse
      .addInternalServerError
      .addServiceUnavailabeError

  def operationForGet(viewDef: ViewDef): Operation =
    createOperation("get", viewDef)
      .addParameters("get", viewDef)
      .addSuccessResponse(view = viewDef.name)
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addNotFoundResponse
      .addInternalServerError
      .addServiceUnavailabeError

  def operationForList(viewDef: ViewDef, keySize: Int = 99): Operation = {
    createOperation("list", viewDef, keySize)
      .addParameters("list", viewDef, keySize)
      .addSuccessResponse(view = viewDef.name, array = true)
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addInternalServerError
      .addServiceUnavailabeError
  }

  def operationForInsert(viewDef: ViewDef): Operation = {
    createOperation("insert", viewDef)
      .addParameters("insert", viewDef)
      .addSuccessResponse(view = viewDef.name)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "insert"))
      .addBadRequestResponse
      .addInternalServerError
      .addServiceUnavailabeError
  }

  def operationForUpdate(viewDef: ViewDef): Operation = {
    createOperation("update", viewDef)
      .addParameters("update", viewDef)
      .addSuccessResponse(view = viewDef.name)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "update"))
      .addBadRequestResponse
      .addInternalServerError
      .addServiceUnavailabeError
  }

  def operationForSave(viewDef: ViewDef): Operation = {
    createOperation("save", viewDef)
      .addParameters("save", viewDef)
      .addSuccessResponse(view = viewDef.name)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "save"))
      .addBadRequestResponse
      .addInternalServerError
      .addServiceUnavailabeError
  }

  def operationForDelete(viewDef: ViewDef): Operation = {
    createOperation("delete", viewDef)
      .addParameters("delete", viewDef)
      .addNotFoundResponse
      .addInternalServerError
  }

  def ungroupedOperations(method: String, viewDef: ViewDef): Seq[(String, String, Operation)] =
    method match {
      case "create" => Seq((pathWithKey(method, viewDef), "GET",    operationForCreate(viewDef)))
      case "count"  => Seq((pathWithKey(method, viewDef), "GET",    operationForCount(viewDef)))
      case "get"    => Seq((pathWithKey(method, viewDef), "GET",    operationForGet(viewDef)))
      case "list"   =>
        (viewDef.minKeySizeForList to viewDef.maxKeySizeForList).map { keySize =>
          (pathWithKey(method, viewDef, keySize),         "GET",    operationForList(viewDef, keySize)
        )}
      case "insert" => Seq((pathWithKey(method, viewDef), "POST",   operationForInsert(viewDef)))
      case "update" => Seq((pathWithKey(method, viewDef), "PUT",    operationForUpdate(viewDef)))
      case "save"   => Seq((pathWithKey(method, viewDef), "PUT",    operationForSave(viewDef)))
      case "delete" => Seq((pathWithKey(method, viewDef), "DELETE", operationForDelete(viewDef)))
      case _        =>
        logger.warn(
          s"Unsupported api method '$method'. View ${viewDef.name}, " +
          s"key fields ${viewDef.keyFieldNames.mkString("[", ", ", "]")}")
        Nil
    }

  def ungroupedOperations: Seq[(String, String, Operation)] =
    viewdefs.flatMap { viewDef =>
      viewDef.apiMethodToRoles.keys.toList.flatMap { method =>
        ungroupedOperations(method, viewDef)
      }
    }

  def pathsFromViewDefs: Seq[(String, PathItem)] = {
    ungroupedOperations.groupBy(_._1).map { case (key, listOfOperations) =>
      val pi = new PathItem
      listOfOperations.foreach { case (_, operationKey, operation) =>
        operationKey match {
          case "GET" => pi.setGet(operation)
          case "PUT" => pi.setPut(operation)
          case "POST" => pi.setPost(operation)
          case "DELETE" => pi.setDelete(operation)
        }
      }
      key -> pi
    }.toSeq
  }.sortBy(_._1)

  def dropIrrelevant(qe: Querease, views: List[ViewDef]): List[ViewDef] = {
    val relevantViewsQueue    = collection.mutable.Queue[ViewDef]()
    val relevantViewNamesSet  = collection.mutable.Set[String]()
    relevantViewsQueue ++= (views.filter(isRelevantView))
    while (relevantViewsQueue.nonEmpty) {
      val currentView = relevantViewsQueue.dequeue()
      if (!relevantViewNamesSet.contains(currentView.name)) {
        currentView.fields
          .filter(_.type_.isComplexType)
          .foreach { field =>
            relevantViewsQueue.enqueue(qe.nameToViewDef(field.type_.name))
          }
        relevantViewNamesSet.add(currentView.name)
      }
    }
    views.filter(v => relevantViewNamesSet(v.name))
  }

  def swaggerDocument: OpenAPI = {
    val openapi = swaggerConfig
    val paths = if (openapi.getPaths == null) {
      val p = new Paths
      openapi.setPaths(p)
      p
    } else openapi.getPaths
    pathsFromViewDefs.foreach { i =>
      paths.addPathItem(i._1, i._2)
    }
    val components = if (openapi.getComponents == null) {
      val p = new Components
      openapi.setComponents(p)
      p
    } else openapi.getComponents

    schemasFromViewDefs.toSeq.sortBy(_._1).foreach { i =>
      components.addSchemas(i._1, i._2)
    }

    openapi
  }

  def generateSwaggerJson: String = {
    try {
      val objectWriter = if (specVersion == SpecVersion.V31) Json31.pretty() else Json.pretty()
      objectWriter.writeValueAsString(swaggerDocument)
    } catch {
      case NonFatal(t) => {
        logger.error("Failed to generate swagger.json", t)
        throw t
      }
    }
  }

  def generateSwaggerYaml: String = {
    try {
      val objectWriter = if (specVersion == SpecVersion.V31) Yaml31.pretty() else Yaml.pretty()
      objectWriter.writeValueAsString(swaggerDocument)
    } catch {
      case NonFatal(t) => {
        logger.error("Failed to generate swagger.yaml", t)
        throw t
      }
    }
  }
}
