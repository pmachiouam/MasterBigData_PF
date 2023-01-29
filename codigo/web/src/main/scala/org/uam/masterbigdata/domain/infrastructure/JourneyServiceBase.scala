package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.model.Entities.Journey
import org.uam.masterbigdata.domain.service.JourneysService

import scala.concurrent.Future


class JourneyServiceBase(modelService: ModelService[Future]) extends JourneysService{
  override def getJourney(id: Long): Future[Journey] = modelService.findJourneyById(id)

  override def getJourney(label: String): Future[Seq[Journey]] = modelService.findJourneysByLabel(label)

  override def getAllJourneys(): Future[Seq[Journey]]= modelService.findAllJourneys()

}

object JourneyServiceBase{
  def apply(modelService: ModelService[Future]) = new JourneyServiceBase(modelService)
}
