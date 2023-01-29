package org.uam.masterbigdata.domain.service

import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.error.DomainError

import scala.concurrent.Future


trait
JourneysService {
 def getJourney(id:Long):Future[Either[DomainError, JourneyView]]
  def getJourney(label:String):Future[Either[DomainError,Seq[JourneyView]]]
 def getAllJourneys():Future[Either[DomainError,Seq[JourneyView]]]

}

object JourneysService
