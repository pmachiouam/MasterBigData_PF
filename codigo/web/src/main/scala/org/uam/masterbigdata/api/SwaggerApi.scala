package org.uam.masterbigdata.api

import akka.http.scaladsl.server.Route
import org.uam.masterbigdata.api.documentation.ActuatorEndpoint
import org.uam.masterbigdata.api.documentation.ApiEndpoint.{apiResourceName, apiVersionName}
import org.uam.masterbigdata.api.model.BuildInfo
import sttp.tapir.docs.openapi._
import sttp.tapir.openapi.Info
import sttp.tapir.openapi.circe.yaml._
import sttp.tapir.swagger.akkahttp.SwaggerAkka
trait SwaggerApi {

  private lazy val endpoints = Seq(
    // A C T U A T O R  E N D P O I N T
    ActuatorEndpoint.healthEndpoint,
  )

  private lazy val info = Info(BuildInfo.name, BuildInfo.version)

  private lazy val docsAsYaml: String =
    OpenAPIDocsInterpreter
      .toOpenAPI(endpoints, info)
      .toYaml

  lazy val route: Route =
    new SwaggerAkka(docsAsYaml, s"$apiResourceName/$apiVersionName/docs").routes

}

object SwaggerApi extends SwaggerApi
