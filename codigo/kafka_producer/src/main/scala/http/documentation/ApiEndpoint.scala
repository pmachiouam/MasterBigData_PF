package http.documentation

import sttp.tapir.{Endpoint, EndpointInput, _}

trait ApiEndpoint {
  private[api] lazy val apiResource: String = "api"
  private[api] lazy val apiVersion: String = "v1.0"
  private[api] lazy val apiNameResource: String = "api-resource"
  private[api] lazy val apiDescriptionResource: String = "Api Resources"
  private[api] lazy val baseApiResource: EndpointInput[Unit] = apiResource / apiVersion

  //urls para los modelos (Se llaman desde sus respectivas apis)
  private[api] lazy val kafkaProducerResourceName:String = "kafkaproducer"


  private[api] lazy val deviceIdPath = path[Long]("deviceId")
  private[api] lazy val objectIdPath = path[String]("objectId")
  private[api] lazy val journeysResource: EndpointInput[Long] = kafkaProducerResourceName / deviceIdPath


  // E N D P O I N T
  private[api] lazy val baseEndpoint: Endpoint[Unit, Unit, Unit, Nothing] =
    endpoint
      .in(baseApiResource)
      .name(apiNameResource)
      .description(apiDescriptionResource)

}

object ApiEndpoint extends ApiEndpoint
