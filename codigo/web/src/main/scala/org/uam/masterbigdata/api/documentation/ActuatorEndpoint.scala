package org.uam.masterbigdata.api.documentation

import io.circe.generic.auto._
import org.uam.masterbigdata.api.ApiModel.BuildInfoDto
import org.uam.masterbigdata.api.Converters
import org.uam.masterbigdata.api.documentation.ApiEndpoint._
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir._

trait ActuatorEndpoint {

  // I N F O R M A T I O N
  type HealthInfo = BuildInfoDto

  // E N D P O I N T
  private[api] lazy val healthEndpoint: Endpoint[Unit, StatusCode, HealthInfo, Any] =
    baseEndpoint
      .get
      .in("health")
      .name("health-resource")
      .description("Vertex Composer Service Health Check Endpoint")
      .out(jsonBody[HealthInfo].example(Converters.buildInfoToApi()))
      .errorOut(statusCode)
}

object ActuatorEndpoint extends ActuatorEndpoint
