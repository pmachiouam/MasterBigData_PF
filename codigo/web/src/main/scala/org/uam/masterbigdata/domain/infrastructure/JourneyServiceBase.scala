package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.model.Entities
import org.uam.masterbigdata.domain.model.Entities.Journey
import org.uam.masterbigdata.domain.service.JourneysService

import scala.concurrent.Future


class JourneyServiceBase(modelService: ModelService[Future]) extends JourneysService{

 /* override def getJourney(id: Long): Future[Journey] = modelService.findJourneyById(id)

  override def getJourney(label: String): Future[Seq[Journey]] = modelService.findJourneysByLabel(label)

  override def getAllJourneys(): Future[Seq[Journey]]= modelService.findAllJourneys()
*/

  override def getDeviceJourney(request: Entities.JourneyByDeviceIdRequest): Future[Journey] =
    modelService.findJourneyById(request.deviceId, request.journeyId)

  override def getAllDeviceJourneysByLabel(request: Entities.JourneysByDeviceIdAndLabelRequest): Future[Seq[Journey]] =
    modelService.findJourneysByLabel(request.deviceId, request.label)

  override def getAllDeviceJourneys(request: Entities.JourneysByDeviceIdRequest): Future[Seq[Journey]] =
    modelService.findAllJourneys(request.deviceId)
}

object JourneyServiceBase{
  def apply(modelService: ModelService[Future]) = new JourneyServiceBase(modelService)
}
