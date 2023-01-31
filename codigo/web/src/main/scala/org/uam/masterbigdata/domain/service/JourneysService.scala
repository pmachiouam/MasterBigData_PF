package org.uam.masterbigdata.domain.service

import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.Entities.{Journey, JourneyByDeviceIdRequest, JourneysByDeviceIdAndLabelRequest, JourneysByDeviceIdRequest}
import org.uam.masterbigdata.domain.model.error.DomainError

import scala.concurrent.Future


trait
JourneysService {
 def getDeviceJourney(request:JourneyByDeviceIdRequest):Future[Journey]
 def getAllDeviceJourneysByLabel(request:JourneysByDeviceIdAndLabelRequest):Future[Seq[Journey]]
 def getAllDeviceJourneys(request:JourneysByDeviceIdRequest):Future[Seq[Journey]]

}

object JourneysService
