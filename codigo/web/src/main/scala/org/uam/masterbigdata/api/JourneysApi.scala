package org.uam.masterbigdata.api

import org.uam.masterbigdata.api.documentation.{ApiMapper, JourneysEndpoint}
import org.uam.masterbigdata.domain.service.JourneysService
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import sttp.tapir.server.akkahttp._



class JourneysApi(service:JourneysService) extends ApiMapper {
  lazy val routes: Route = getJourneyByIdRoute ~ getJourneyByLabelRoute ~ getAllJourneysRoute

  private lazy val getJourneyByIdRoute: Route = JourneysEndpoint.getJourneyByIdEndpoint.toRoute {
    mapToId andThen service.getJourney
  }

  private lazy val getJourneyByLabelRoute: Route = JourneysEndpoint.getJourneysByLabelEndpoint.toRoute {
    mapToLabel andThen service.getJourney
  }

  private lazy val getAllJourneysRoute: Route = JourneysEndpoint.getJourneysEndpoint.toRoute {
    _ => service.getAllJourneys()
  }

}

object JourneysApi{
  def apply(service:JourneysService):JourneysApi = new JourneysApi(service)
}
