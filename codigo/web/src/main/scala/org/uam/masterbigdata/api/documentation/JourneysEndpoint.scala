package org.uam.masterbigdata.api.documentation



import org.uam.masterbigdata.api.documentation.ApiEndpoint.{baseEndpoint, journeysResource, objectIdPath}
import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.error.DomainError
import sttp.tapir.{Endpoint, EndpointInput, _}

trait JourneysEndpoint extends JsonCodecs  with ApiErrorMapping {

  private lazy  val journeyByIdResource: EndpointInput[Long] = journeysResource / objectIdPath
  lazy val getJourneyByIdEndpoint: Endpoint[Long, DomainError, JourneyView, Nothing] =
    baseEndpoint
      .get
      .name("GetJourneyById")
      .description("Recupera un trayecto por su identificador")
      .in(journeyByIdResource)
      .out(jsonBody[JourneyView])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))

  private lazy val objectLabelPath = path[String]("label")
  private lazy val journeyByNameResource: EndpointInput[String] = journeysResource / objectLabelPath
  lazy val getJourneysByLabelEndpoint: Endpoint[String, DomainError, Seq[JourneyView], Nothing] =
    baseEndpoint
      .get
      .name("GetJourneyByLabel")
      .description("Recupera todos los trayectos que tienen la misma etiqueta")
      .in(journeyByNameResource)
      .out(jsonBody[Seq[JourneyView]])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))
/*
  private[api] lazy val journeysHowManyQuery = query[Option[Int]]("howMany")
  private[api] lazy val journeysHowManyQueryResource:EndpointInput[Option[Int]] =journeysResource / journeysHowManyQuery
  lazy val getJourneysEndpoint: Endpoint[Option[Int], DomainError, Seq[JourneyView], Nothing] =
    baseEndpoint
      .get
      .name("GetAllJourneys")
      .description("Recupera todos los trayectos")
      .in(journeysHowManyQueryResource)
      .out(jsonBody[Seq[JourneyView]])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))
*/

}

object JourneysEndpoint extends JourneysEndpoint{
  //private val journeyViewExample: JourneyView = JourneyView("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11")
}