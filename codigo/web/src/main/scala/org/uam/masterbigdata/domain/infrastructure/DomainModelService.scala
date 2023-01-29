package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.domain.infrastructure.model.Entities.JourneyDbo
import org.uam.masterbigdata.domain.model.Entities.Journey

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


final  class DomainModelService(dal: DataAccessLayer) extends ModelService[Future] with ComponentLogging {
  import dal.executeOperation

  override def findAllJourneys(): Future[Seq[Journey]] = {
    log.info("Buscando todos los trayectos")
    for {
      dbo <- dal.journeysRepository.find()
    } yield dbo.map(dboToModel)
  }


  override def findJourneysByLabel(label: String): Future[Seq[Journey]] = {
    log.info(s"Buscando todos los trayectos por label $label")
    for {

      dbo <- dal.journeysRepository.find(label)
    } yield dbo.map(dboToModel)
  }

  override def findJourneyById(id: Long): Future[Journey] = {
    log.info(s"Buscando todos los trayectos por id $id")
    for {
      dbo <- dal.journeysRepository.find(id)
    } yield dboToModel(dbo)
  }

  private def dboToModel(dbo: JourneyDbo):Journey =
    Journey(dbo.id, dbo.device_id.toString, dbo.start_timestamp.toString, dbo.start_location_address, dbo.start_location_latitude.toString
      ,dbo.start_location_longitude.toString, dbo.end_timestamp.toString, dbo.end_location_address, dbo.end_location_latitude.toString
      , dbo.end_location_longitude.toString, dbo.distance.toString)
}

object DomainModelService {
  def apply(dal: DataAccessLayer) = new DomainModelService(dal)
}