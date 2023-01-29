package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.AdapterModel
import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.error.DomainError
import org.uam.masterbigdata.domain.service.JourneysService

import scala.concurrent.Future


class JourneyServiceBase extends JourneysService{
  override def getJourney(id: Long): Future[Either[DomainError, JourneyView]] = Future.successful(Right(JourneyView("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K")))

  override def getJourney(label: String): Future[Either[DomainError, Seq[JourneyView]]] = Future.successful(Right(Seq(JourneyView("AA", "BB", "C", "D", "E", "F", "G", "H", "I", "J", "K"))))


  override def getAllJourneys(): Future[Either[DomainError, Seq[JourneyView]]] = Future.successful(Right(Seq(JourneyView("AAA", "BBB", "C", "D", "E", "F", "G", "H", "I", "J", "K"))))

}

object JourneyServiceBase{
  def apply() = new JourneyServiceBase
}
