package org.uam.masterbigdata.api

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.DebuggingDirectives
import org.uam.masterbigdata.api.documentation.ActuatorEndpoint
import sttp.tapir.server.akkahttp.AkkaHttpServerInterpreter

/**
 * Actuator endpoint for monitoring application
 * http://host:port/api/v1.0/health
 */
trait ActuatorApi {

  // https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/debugging-directives/logRequestResult.html
  lazy val route: Route = DebuggingDirectives.logRequestResult("api-logger") {
    AkkaHttpServerInterpreter.toRoute(ActuatorEndpoint.healthEndpoint)(_ =>
      Converters.asRightFuture(Converters.buildInfoToApi())
    )
  }

}

object ActuatorApi extends ActuatorApi
