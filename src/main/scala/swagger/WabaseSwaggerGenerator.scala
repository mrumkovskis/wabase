package org.wabase.swagger

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import io.swagger.v3.core.util.{Json, Json31, Yaml, Yaml31}
import io.swagger.v3.oas.models._
import io.swagger.v3.oas.models.callbacks.Callback
import io.swagger.v3.oas.models.examples.Example
import io.swagger.v3.oas.models.headers.Header
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.links.Link
import io.swagger.v3.oas.models.media._
import io.swagger.v3.oas.models.parameters.{Parameter, PathParameter, QueryParameter, RequestBody}
import io.swagger.v3.oas.models.responses.{ApiResponse, ApiResponses}
import io.swagger.v3.oas.models.security.{SecurityRequirement, SecurityScheme}
import io.swagger.v3.oas.models.servers.Server
import org.apache.commons.lang3.StringUtils
import org.apache.pekko.http.scaladsl.model.{HttpMethod, HttpMethods, StatusCodes}
import org.mojoz.metadata.{FieldDef, Type, ViewDef}
import org.mojoz.querease.FilterType.{ComparisonFilter, OtherFilter}
import org.mojoz.querease.Querease
import org.wabase.AppMetadata.Action.{Evaluation, Validations, ViewCall}
import org.wabase.AppMetadata.{AugmentedAppFieldDef, AugmentedAppViewDef, FilterParameter, PathNameAndParameters, RouteDef}
import org.wabase.{AppMetadata, AppQuerease, Loggable, MapUtils}

import java.net.URI
import java.util.{List => JList, Map => JMap}
import scala.collection.immutable.{Map, TreeMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.language.existentials
import scala.util.control.NonFatal
import scala.util.Try

class WabaseSwaggerGenerator(
  qes: Seq[Querease],
  hostString: String,
  isRelevantView:     ViewDef  => Boolean = _.apiMethodToRoles.nonEmpty,
  isRelevantRoute:    RouteDef => Boolean = _ => true,
  config: Config = org.wabase.config,
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

  val marshalKeyAsJson: Boolean =
    Option("app.marshal_key_as_json").filter(config.hasPath).map(config.getBoolean).getOrElse(true)

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

  lazy val viewNameToApiKeyFieldNames: Map[String, Seq[String]] = viewdefs.map { v => (
    v.name,
    viewNameToQe(v.name) match {
      case qe: AppQuerease => qe.viewNameToApiKeyFieldNames(v.name)
      case qe              => qe.viewNameToKeyFields(v.name).filterNot(_.api.excluded).map(_.fieldName)
    }
  )}.toMap

  def apiKeyFieldNames(viewDef: ViewDef) = viewNameToApiKeyFieldNames(viewDef.name)

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
    case "object" => new ObjectSchema
    case "string" =>
      val s = new StringSchema
      type_.length.foreach(l => s.maxLength(l))
      s
    case "json" => new ObjectSchema
    case "yaml" => new StringSchema
    case n if n.endsWith("String") => new StringSchema
    case n => (new ObjectSchema).`type`(n)
  }

  def typeNameToSchema(typeName: String): Schema[_] = {
    schemaFromType(
      new Type(typeName).copy(
        isComplexType = qes.exists(_.nameToViewDef.contains(typeName)),
      )
    )
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
    val defaultSchema = maybeArraySchema
      .name(fieldName)
      .readOnly(getReadOnly(viewdefs)(field))
      .description(Option(field.comments).getOrElse(field.label))
    val fieldSchemaOverrides =
      field.extras.get(swaggerOverridesKey).map {
        case m: Map[String @unchecked, _] => m
        case x =>
          throw new RuntimeException(
            s"Unexpected class for value of $swaggerOverridesKey in field ${field.fieldName}." +
            s" Expecting map, got ${Option(x).map(_.getClass.getName).orNull}")
      }.getOrElse(Map.empty)
    fieldName -> SwaggerMerger.mergeSchema(
      defaultSchema,
      MapUtils.mapToJavaMap(fieldSchemaOverrides).asInstanceOf[JMap[String, Object]],
      typeNameToSchema,
    )
  }

  def isApiField(f: FieldDef): Boolean =
    !f.api.excluded

  def shouldIncludeSchemaForView(v: ViewDef): Boolean = true
  def schemasFromViewDefs(viewDefMap: Map[String, ViewDef]): Map[String, Schema[_]] = {
    viewDefMap.view.filter { case (_, v) => shouldIncludeSchemaForView(v) }.flatMap { case (viewName, viewDef) =>
      val filteredFields = viewDef.fields.filter(isApiField)
      val fields: TreeMap[String, Schema[_]] =
        TreeMap()(viewNameToQe(viewDef.name).fieldOrdering(viewDef.name)) ++
        filteredFields.map(schemaFromFieldDef(viewDefMap)).toMap
      val fieldsAsJava = new java.util.LinkedHashMap[String, Schema[_]](fields.size, 1)
      fields.foreach { case (name, schema) => fieldsAsJava.put(name, schema) }
      val requiredFields = filteredFields.filter(fieldRequired(viewDefMap)).map(_.fieldName).toList
      val viewSchema = new ObjectSchema()
        .name(viewName)
        .description(viewDef.comments)
        .properties(fieldsAsJava)
        .required(Option(requiredFields).filter(_.nonEmpty).map(_.asJava).orNull)
        .asInstanceOf[Schema[Object]] /* cast for scala 2.12 */
      if (marshalKeyAsJson && hasKeyResultMethods(viewDef) && apiKeyFieldNames(viewDef).nonEmpty) {
        val keyFields: TreeMap[String, Schema[_]] =
          TreeMap()(viewNameToQe(viewDef.name).fieldOrdering(viewDef.name)) ++
          apiKeyFieldNames(viewDef).map { keyFieldName =>
            viewDef.fieldOpt(keyFieldName).getOrElse(
              new org.mojoz.metadata.FieldDef(keyFieldName, new org.mojoz.metadata.Type("string")))
          }.map(schemaFromFieldDef(viewDefMap)).toMap
        val keyFieldsAsJava = new java.util.LinkedHashMap[String, Schema[_]](fields.size, 1)
        keyFields.foreach { case (name, schema) => keyFieldsAsJava.put(name, schema) }
        val keyResponseName = keySchemaName(viewDef.name)
        val keySchema = new ObjectSchema()
          .name(keyResponseName)
          .properties(keyFieldsAsJava)
          .asInstanceOf[Schema[Object]] /* cast for scala 2.12 */
        Seq(viewName -> viewSchema, keyResponseName -> keySchema)
      } else Seq(viewName -> viewSchema)
    }.toMap - "count" // no object schema for "count" service
  }

  val schemaRefPrefix = "#/components/schemas/"
  def refFromViewName(viewName: String) = s"${schemaRefPrefix}${viewName}"
  def keyRefFromViewName(viewName: String) = s"${schemaRefPrefix}${keySchemaName(viewName)}"
  def viewNameFromRef(ref: String) = if (ref.startsWith(schemaRefPrefix)) ref.substring(schemaRefPrefix.length) else ref

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

  def jsonKeyContent(view: String, array: Boolean = false): Content = {
    val content = new Content
    val mediaType = new MediaType
    val viewSchema = (new Schema()).$ref(keyRefFromViewName(view))
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

  def keyResponseContent(view: String, array: Boolean = false) =
    jsonKeyContent(view, array)

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
    apiKeyFieldNames(viewDef).take(keySize).foreach { keyFieldName =>
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

  def addPathParameter(op: Operation, pathParameter: AppMetadata.PathParameter): Operation = {
    val p = new PathParameter
    p.name(pathParameter.name)
    val schema =
      if  (pathParameter.typeName == null)
           new StringSchema
      else schemaFromType(new Type(pathParameter.typeName))
    if (pathParameter.pattern != null && pathParameter.pattern != "^.*$")
      schema.setPattern(pathParameter.pattern)
    p.setSchema(schema)
    op.addParametersItem(p)
    op
  }

  def addPathParameters(op: Operation, pathInfo: PathNameAndParameters): Operation = {
    pathInfo.parameters.foreach(addPathParameter(op, _))
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

  def addParameters(op: Operation, pathInfo: PathNameAndParameters): Operation = {
    addPathParameters  (op, pathInfo)
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

  private val KeyResultMethodNames = Set("insert", "update", "update+", "upsert", "save", "put", "post")
  def isKeyResultMethod(method: String) = KeyResultMethodNames.contains(method)
  def hasKeyResultMethods(viewDef: ViewDef) = viewDef.apiMethodToRoles.keys.exists(isKeyResultMethod)
  def keySchemaName(viewName: String) = s"${viewName}_key_response"

  def addSuccessResponse(op: Operation, method: String, viewDef: ViewDef, code: String = "200", array: Boolean = false): Operation = {
    val responses = getResponses(op)
    val response = new ApiResponse
    if (marshalKeyAsJson && viewDef != null && isKeyResultMethod(method)) {
      if (apiKeyFieldNames(viewDef).nonEmpty)
        response.content(keyResponseContent(viewDef.name, array))
    } else if (viewDef != null && hasApiFields(viewDef.name)) {
      response.content(responseContent(viewDef.name, array))
    }
    responses.addApiResponse(code, response)
    op
  }

  def addSuccessResponse(op: Operation, method: HttpMethod): Operation = method match {
    case HttpMethods.DELETE => addSuccessResponse(op, method.value.toLowerCase, null, "204")
    case HttpMethods.POST   => addSuccessResponse(op, method.value.toLowerCase, null, "201")
    case _                  => addSuccessResponse(op, method.value.toLowerCase, null, "200")
  }

  def addSuccessPlaintextResponse(op: Operation, view: String, code: String = "200"): Operation = {
    val responses = getResponses(op)
    val response = new ApiResponse
    response.content(plaintextContent(view))
    responses.addApiResponse(code, response)
    op
  }

  def addErrorResponse(op: Operation, code: String, description: String = null, content: Content = null): Operation = {
    val responses = getResponses(op)
    val response = new ApiResponse
    response.description(description)
    if (content != null) response.content(content)
    responses.addApiResponse(code, response)
    op
  }

  def addBadRequestResponse(op: Operation)     = addErrorResponse(op, "400")
  def addForbiddenResponse(op: Operation, viewDef: ViewDef) = addErrorResponse(op, "403")
  def addNotFoundResponse(op: Operation)       = addErrorResponse(op, "404")
  def addInternalServerError(op: Operation)    = addErrorResponse(op, "500")
  def addServiceUnavailabeError(op: Operation) = addErrorResponse(op, "503")

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
    def addParameters(pathInfo: PathNameAndParameters): Operation =
          delegate.addParameters(op, pathInfo)
    def addPathParameter(pathParameter: AppMetadata.PathParameter): Operation =
          delegate.addPathParameter(op, pathParameter)
    def addPathParameters(pathInfo: PathNameAndParameters): Operation =
          delegate.addPathParameters(op, pathInfo)
    def addQueryParameter(param: FilterParameter): Operation =
          delegate.addQueryParameter(op, param)
    def addQueryParameters(method: String, viewDef: ViewDef, keySize: Int = 99): Operation =
          delegate.addQueryParameters(op, method, viewDef, keySize)
    def addRequestBody(view: String, array: Boolean = false): Operation = delegate.addRequestBody(op, view, array)
    def addServiceUnavailabeError = delegate.addServiceUnavailabeError(op)
    def addSuccessPlaintextResponse(view: String, code: String = "200"): Operation =
          delegate.addSuccessPlaintextResponse(op, view, code)
    def addSuccessResponse(method: String, viewDef: ViewDef, code: String = "200", array: Boolean = false): Operation =
          delegate.addSuccessResponse(op, method, viewDef, code, array)
    def addSuccessResponse(method: HttpMethod): Operation =
          delegate.addSuccessResponse(op, method)
    def getResponses: ApiResponses = delegate.getResponses(op)
  }

  def getErrorCodesForView(viewDef: ViewDef, action: String): List[String] = {
    val steps = viewDef.actions.get(action).map(_.steps).getOrElse(Nil)
    val res = steps.flatMap {
      case (Validations(Some(name), _, _), _) => List(name)
      case (Evaluation(_, _, ViewCall(method, view, _, _)), _) if view != viewDef.name =>
        viewNameToQe(view).nameToViewDef.get(view).map(subView =>
          getErrorCodesForView(subView, method)
        ).getOrElse(Nil)
      case _ => Nil
    }
    res
  }

  def keyDescription(viewDef: ViewDef, keySize: Int = 99): String = {
    val keyFieldNames = apiKeyFieldNames(viewDef).take(keySize)
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
      case "delete" => null
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
    if (apiKeyFieldNames(viewDef).take(keySize).isEmpty)
      s"${rootPathForView(viewDef)}$infix"
    else
      s"${rootPathForView(viewDef)}$infix/${apiKeyFieldNames(viewDef).take(keySize).mkString("{", "}/{", "}")}"
  }

  def isArrayRequest(viewDef: ViewDef, method: String) = false

  def operationForCreate(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("create", viewDef, keySize)
      .addParameters("create", viewDef, keySize)
      .addSuccessResponse("create", viewDef)
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addNotFoundResponse
      .addServiceUnavailabeError

  def operationForCount(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("count", viewDef, keySize)
      .addParameters("count", viewDef, keySize)
      .addIntegerResponse("Count")
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addNotFoundResponse
      .addServiceUnavailabeError

  def operationForGet(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("get", viewDef, keySize)
      .addParameters("get", viewDef, keySize)
      .addSuccessResponse("get", viewDef)
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addNotFoundResponse
      .addServiceUnavailabeError

  def operationForList(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("list", viewDef, keySize)
      .addParameters("list", viewDef, keySize)
      .addSuccessResponse("list", viewDef, array = true)
      .addBadRequestResponse
      .addForbiddenResponse(viewDef)
      .addServiceUnavailabeError

  def operationForInsert(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("insert", viewDef, keySize)
      .addParameters("insert", viewDef, keySize)
      .addSuccessResponse("insert", viewDef)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "insert"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForUpdate(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("update", viewDef, keySize)
      .addParameters("update", viewDef, keySize)
      .addSuccessResponse("update", viewDef)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "update"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForUpdatePlus(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("update+", viewDef, keySize)
      .addParameters("update+", viewDef, keySize)
      .addSuccessResponse("update+", viewDef)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "update+"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForUpsert(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("upsert", viewDef, keySize)
      .addParameters("upsert", viewDef, keySize)
      .addSuccessResponse("upsert", viewDef)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "upsert"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForSave(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("save", viewDef, keySize)
      .addParameters("save", viewDef, keySize)
      .addSuccessResponse("save", viewDef)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "save"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForDelete(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("delete", viewDef, keySize)
      .addParameters("delete", viewDef, keySize)
      .addSuccessResponse(HttpMethods.DELETE)
      .addNotFoundResponse

  def operationForPut(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("put", viewDef, keySize)
      .addParameters("put", viewDef, keySize)
      .addSuccessResponse("put", viewDef)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "put"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForPost(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("post", viewDef, keySize)
      .addParameters("post", viewDef, keySize)
      .addSuccessResponse("post", viewDef)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "post"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForHead(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("head", viewDef, keySize)
      .addParameters("head", viewDef, keySize)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "head"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForOptions(viewDef: ViewDef, keySize: Int = 99): Operation =
    createOperation("options", viewDef, keySize)
      .addParameters("options", viewDef, keySize)
      .addRequestBody(view = viewDef.name, isArrayRequest(viewDef, "options"))
      .addBadRequestResponse
      .addServiceUnavailabeError

  def operationForDelete (pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.DELETE)
  def operationForGet    (pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.GET)
  def operationForHead   (pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.HEAD)
  def operationForOptions(pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.OPTIONS)
  def operationForPatch  (pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.PATCH)
  def operationForPost   (pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.POST)
  def operationForPut    (pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.PUT)
  def operationForTrace  (pathInfo: PathNameAndParameters): Operation = new Operation().addParameters(pathInfo).addSuccessResponse(HttpMethods.TRACE)

  private val allSupportedHttpMethods = Set(
    HttpMethods.DELETE,
    HttpMethods.GET,
    HttpMethods.HEAD,
    HttpMethods.OPTIONS,
    HttpMethods.PATCH,
    HttpMethods.POST,
    HttpMethods.PUT,
    HttpMethods.TRACE,
  )
  def defaultHttpMethodsForRoute: Set[HttpMethod] = allSupportedHttpMethods

  def setOperation(pathItem: PathItem, method: HttpMethod, operation: Operation): PathItem = method match {
    case HttpMethods.DELETE  => pathItem.delete (operation)
    case HttpMethods.GET     => pathItem.get    (operation)
    case HttpMethods.HEAD    => pathItem.head   (operation)
    case HttpMethods.OPTIONS => pathItem.options(operation)
    case HttpMethods.PATCH   => pathItem.patch  (operation)
    case HttpMethods.POST    => pathItem.post   (operation)
    case HttpMethods.PUT     => pathItem.put    (operation)
    case HttpMethods.TRACE   => pathItem.trace  (operation)
    case x => throw new RuntimeException(s"Http method not supported by swagger generator: $x") // not expected
  }

  private val fullKeyOps = Set("get",/*insert*/ "update", "update+", "upsert", "save", "delete", "put")
  def pathsAndOperations(method: String, viewDef: ViewDef): Seq[(String, HttpMethod, Operation)] = {
    lazy val hasFullKeyOps = fullKeyOps.exists(viewDef.apiMethodToRoles.contains)
    lazy val z = if (hasFullKeyOps) 0 else 99
    method match {
      case "create" => Seq((pathWithKey(method, viewDef, 0), HttpMethods.GET, operationForCreate(viewDef, 0)))
      case "count"  => Seq((pathWithKey(method, viewDef, viewDef.maxKeySizeForList),
                            HttpMethods.GET,
                            operationForCount(viewDef, viewDef.maxKeySizeForList)))
      case "get"    => Seq((pathWithKey(method, viewDef), HttpMethods.GET,    operationForGet(viewDef)))
      case "list"   =>
        (viewDef.minKeySizeForList to viewDef.maxKeySizeForList).map { keySize =>
          (pathWithKey(method, viewDef, keySize),         HttpMethods.GET,    operationForList(viewDef, keySize)
        )}
      case "insert" => Seq((pathWithKey(method, viewDef, z), HttpMethods.POST,operationForInsert(viewDef, z)))
      case "update" => Seq((pathWithKey(method, viewDef), HttpMethods.PUT,    operationForUpdate(viewDef)))
      case "update+"=> Seq((pathWithKey(method, viewDef), HttpMethods.POST,   operationForUpdatePlus(viewDef)))
      case "upsert" => Seq((pathWithKey(method, viewDef), HttpMethods.PUT,    operationForUpsert(viewDef)))
      case "save"   =>
                if  (apiKeyFieldNames(viewDef).isEmpty)
                       Seq((pathWithKey(method, viewDef),    HttpMethods.POST, operationForSave(viewDef)))
                else   Seq((pathWithKey(method, viewDef, 0), HttpMethods.POST, operationForInsert(viewDef, 0)),
                           (pathWithKey(method, viewDef),    HttpMethods.PUT,  operationForUpdate(viewDef)))
      case "delete" => Seq((pathWithKey(method, viewDef),    HttpMethods.DELETE,  operationForDelete(viewDef)))
      case "put"    => Seq((pathWithKey(method, viewDef),    HttpMethods.PUT,     operationForPut(viewDef)))
      case "post"   => Seq((pathWithKey(method, viewDef, z), HttpMethods.POST,    operationForPost(viewDef, z)))
      case "head"   => Seq((pathWithKey(method, viewDef),    HttpMethods.HEAD,    operationForHead(viewDef)))
      case "options"=> Seq((pathWithKey(method, viewDef),    HttpMethods.OPTIONS, operationForOptions(viewDef)))
      case _        =>
        logger.warn(s"Unsupported api method '$method' for view '${viewDef.name}' skipped by swagger generator")
        Nil
    }
  }

  def swaggerOverridesKey = "swagger"

  def pathsFromViewDefs: Seq[(String, PathItem)] = {
    viewdefs.flatMap { viewDef =>
      val pathsAndMethodsAndOps =
        viewDef.apiMethodToRoles.keys.toList.flatMap { method =>
            pathsAndOperations(method, viewDef)
        }
      val defaultPaths =
        pathsAndMethodsAndOps.groupBy(_._1).map { case (pathName, listOfOperations) =>
          val pi = new PathItem
          listOfOperations.foreach { case (_, method, operation) =>
            setOperation(pi, method, operation)
          }
          pathName -> pi
        }.toSeq
      val pathsOverrides =
        viewDef.extras.get(swaggerOverridesKey).map {
          case m: Map[String @unchecked, _] => m
          case x =>
            throw new RuntimeException(
              s"Unexpected class for value of $swaggerOverridesKey in ${viewDef.name}." +
              s" Expecting map, got ${Option(x).map(_.getClass.getName).orNull}")
        }.getOrElse(Map.empty)
      SwaggerMerger.mergePaths(
        defaultPaths,
        MapUtils.mapToJavaMap(pathsOverrides).asInstanceOf[JMap[String, Object]],
        typeNameToSchema,
      )
    }
  }.sortBy(_._1)

  def getPaths(pathsMap: JMap[String, _]): Map[String, PathItem] = {
    val mapper = new ObjectMapper()
    pathsMap.asScala.map { case (key, value) =>
      key -> mapper.convertValue(value, classOf[PathItem])
    }.toMap
  }

  def pathsFromRouteDefs: Seq[(String, PathItem)] = {
    qes.collect { case q: AppQuerease => q }.flatMap(_.routeDefs).filter(isRelevantRoute).flatMap { rd =>
      val defaultPaths =
        rd.pathNamesAndParameters.map { pathInfo =>
          val pi = new PathItem
          Option(rd.methods).filter(_.nonEmpty).getOrElse(defaultHttpMethodsForRoute).collect {
            case method @ HttpMethods.DELETE  => setOperation(pi, method, operationForDelete (pathInfo))
            case method @ HttpMethods.GET     => setOperation(pi, method, operationForGet    (pathInfo))
            case method @ HttpMethods.HEAD    => setOperation(pi, method, operationForHead   (pathInfo))
            case method @ HttpMethods.OPTIONS => setOperation(pi, method, operationForOptions(pathInfo))
            case method @ HttpMethods.PATCH   => setOperation(pi, method, operationForPatch  (pathInfo))
            case method @ HttpMethods.POST    => setOperation(pi, method, operationForPost   (pathInfo))
            case method @ HttpMethods.PUT     => setOperation(pi, method, operationForPut    (pathInfo))
            case method @ HttpMethods.TRACE   => setOperation(pi, method, operationForTrace  (pathInfo))
          }
          pathInfo.name -> pi
        }.toSeq
      val pathsOverrides =
        rd.extras.get(swaggerOverridesKey).map {
          case m: Map[String @unchecked, _] => m
          case x =>
            throw new RuntimeException(
              s"Unexpected class for value of $swaggerOverridesKey." +
              s" Expecting map, got ${Option(x).map(_.getClass.getName).orNull}")
        }.getOrElse(Map.empty)
      if (pathsOverrides.isEmpty)
        defaultPaths
      else
        SwaggerMerger.mergePaths(
          defaultPaths,
          MapUtils.mapToJavaMap(pathsOverrides).asInstanceOf[JMap[String, Object]],
          typeNameToSchema,
        )
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

  def collectRefs(pathItem: PathItem): Set[String] = {
    val refs = mutable.Set[String]()
    extractRefs(pathItem, refs)
    refs.toSet
  }

  private def extractRefs(obj: Any, refs: mutable.Set[String]): Unit = {
    if (obj == null) return
    obj match {
      case p: PathItem =>
        if (p.get$ref != null) refs += p.get$ref
        if (p.getParameters != null) p.getParameters.asScala.foreach(extractRefs(_, refs))
        if (p.getServers != null) p.getServers.asScala.foreach(extractRefs(_, refs))
        extractRefs(p.getGet, refs)
        extractRefs(p.getPost, refs)
        extractRefs(p.getPut, refs)
        extractRefs(p.getDelete, refs)
        extractRefs(p.getOptions, refs)
        extractRefs(p.getHead, refs)
        extractRefs(p.getPatch, refs)
        extractRefs(p.getTrace, refs)
      case o: Operation =>
        if (o.getParameters != null) o.getParameters.asScala.foreach(extractRefs(_, refs))
        extractRefs(o.getRequestBody, refs)
        extractRefs(o.getResponses, refs)
        if (o.getCallbacks != null) o.getCallbacks.values.asScala.foreach(extractRefs(_, refs))
        if (o.getServers != null) o.getServers.asScala.foreach(extractRefs(_, refs))
      case param: Parameter =>
        if (param.get$ref != null) refs += param.get$ref
        extractRefs(param.getSchema, refs)
        extractRefs(param.getContent, refs)
      case rb: RequestBody =>
        if (rb.get$ref != null) refs += rb.get$ref
        extractRefs(rb.getContent, refs)
      case responses: ApiResponses =>
        if (responses != null) responses.values.asScala.foreach(extractRefs(_, refs))
      case response: ApiResponse =>
        if (response.get$ref != null) refs += response.get$ref
        extractRefs(response.getContent, refs)
        if (response.getHeaders != null) response.getHeaders.values.asScala.foreach(extractRefs(_, refs))
        if (response.getLinks != null) response.getLinks.values.asScala.foreach(extractRefs(_, refs))
      case content: Content =>
        if (content != null) content.values.asScala.foreach(extractRefs(_, refs))
      case mt: io.swagger.v3.oas.models.media.MediaType =>
        extractRefs(mt.getSchema, refs)
        if (mt.getExamples != null) mt.getExamples.values.asScala.foreach(extractRefs(_, refs))
        if (mt.getEncoding != null) mt.getEncoding.values.asScala.foreach(extractRefs(_, refs))
      case schema: Schema[_] =>
        if (schema.get$ref != null) refs += schema.get$ref
        extractRefs(schema.getNot, refs)
        if (schema.getProperties != null) schema.getProperties.values.asScala.foreach(extractRefs(_, refs))
        extractRefs(schema.getAdditionalProperties, refs)
        extractRefs(schema.getItems, refs)
        if (schema.getAllOf != null) schema.getAllOf.asScala.foreach(extractRefs(_, refs))
        if (schema.getAnyOf != null) schema.getAnyOf.asScala.foreach(extractRefs(_, refs))
        if (schema.getOneOf != null) schema.getOneOf.asScala.foreach(extractRefs(_, refs))
      case header: Header =>
        if (header.get$ref != null) refs += header.get$ref
        extractRefs(header.getSchema, refs)
        extractRefs(header.getContent, refs)
      case link: Link =>
        if (link.get$ref != null) refs += link.get$ref
      case example: Example =>
        if (example.get$ref != null) refs += example.get$ref
      case callback: Callback =>
        callback.values.asScala.foreach(extractRefs(_, refs))
      case l: JList[_] =>
        l.asScala.foreach(extractRefs(_, refs))
      case m: JMap[_, _] =>
        m.values.asScala.foreach(extractRefs(_, refs))
      case _ => // ignore
    }
  }

  def addResponseDescriptions(pathItem: PathItem): PathItem = {
    if (pathItem != null) {
      val operations = pathItem.readOperations().asScala
      operations.foreach { (op: Operation) =>
        val responses: ApiResponses = op.getResponses
        if (responses != null) {
          responses.asScala.foreach { case (codeStr: String, resp: ApiResponse) =>
            if (resp != null && (resp.getDescription == null || resp.getDescription.trim.isEmpty)) {
              val description = codeStr match {
                case "default" => "Default response"
                case c if c.length == 3 && c.toLowerCase.endsWith("xx") =>
                  c.charAt(0) match {
                    case '1' => "Informational response"
                    case '2' => "Successful response"
                    case '3' => "Redirection response"
                    case '4' => "Client error response"
                    case '5' => "Server error response"
                    case _ => "Unknown response"
                  }
                case c =>
                  Try(c.toInt).toOption
                    .flatMap(code => StatusCodes.getForKey(code).map(_.reason()))
                    .getOrElse("Unknown status code")
              }
              resp.setDescription(description)
            }
          }
        }
      }
    }
    pathItem
  }

  def swaggerDocument: OpenAPI = {
    val openapi = swaggerConfig
    val paths = if (openapi.getPaths == null) {
      val p = new Paths
      openapi.setPaths(p)
      p
    } else openapi.getPaths
    val pathNamesAndItems =
      Seq(
        pathsFromRouteDefs,
        pathsFromViewDefs,
      )
        .flatMap(identity)
    pathNamesAndItems.groupBy(_._1).toSeq.sortBy(_._1).map { case (pathName, items) =>
      val mergedItems = SwaggerMerger.mergePathItems(items.map(_._2))
      mergedItems.foreach { pathItem =>
        paths.addPathItem(pathName, addResponseDescriptions(pathItem))
      }
    }
    val components = if (openapi.getComponents == null) {
      val p = new Components
      openapi.setComponents(p)
      p
    } else openapi.getComponents

    val refs = pathNamesAndItems.map(_._2).flatMap(collectRefs).toSet
    val viewNamesFromRefs = refs.map(viewNameFromRef)
    val referencedViews = viewNamesFromRefs.flatMap { v =>
      qes.map(_.nameToViewDef.get(v)).filter(_.nonEmpty).headOption.map(_.get).toSeq
    }
    schemasFromViewDefs(viewDefMap ++ referencedViews.map { v => v.name -> v}).toSeq.sortBy(_._1).foreach { i =>
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
