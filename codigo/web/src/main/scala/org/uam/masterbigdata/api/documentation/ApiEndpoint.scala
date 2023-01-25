package org.uam.masterbigdata.api.documentation

import io.circe.generic.auto._
import org.uam.masterbigdata.api.ApiModel.{BadRequestError, NotFoundError, OutputError, ServerError}
import sttp.model.StatusCode
import sttp.tapir.EndpointOutput.StatusMapping
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.{Endpoint, EndpointInput, _}

import scala.concurrent.Future
trait ApiEndpoint {
  type EndpointResponse[T] = Future[Either[OutputError, T]]

  private[api] lazy val apiResourceName: String = "api"
  private[api] lazy val apiVersionName: String = "v1.0"
  private[api] lazy val apiNameResourceName: String = "api-resource"
  private[api] lazy val apiDescriptionResourceName: String = "Api Resources"

  private[api] lazy val baseApiResource: EndpointInput[Unit] = apiResourceName / apiVersionName

  // E N D P O I N T
  private[api] lazy val baseEndpoint: Endpoint[Unit, Unit, Unit, Any] =
    endpoint
      .in(baseApiResource)
      .name(apiNameResourceName)
      .description(apiDescriptionResourceName)

  /* Error mapping */
  val breMapping: StatusMapping[BadRequestError] = statusMapping(StatusCode.BadRequest, jsonBody[BadRequestError])
  val nfeMapping: StatusMapping[NotFoundError] = statusMapping(StatusCode.NotFound, jsonBody[NotFoundError])
  val iseMapping: StatusMapping[ServerError] = statusMapping(StatusCode.InternalServerError, jsonBody[ServerError])

}

object ApiEndpoint extends ApiEndpoint