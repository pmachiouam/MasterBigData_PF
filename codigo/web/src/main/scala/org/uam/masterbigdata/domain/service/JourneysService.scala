package org.uam.masterbigdata.domain.service

import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.Entities.Journey
import org.uam.masterbigdata.domain.model.error.DomainError

import scala.concurrent.Future


trait
JourneysService {
 def getJourney(id:Long):Future[Journey]
  def getJourney(label:String):Future[Seq[Journey]]
 def getAllJourneys():Future[Seq[Journey]]

}

object JourneysService
