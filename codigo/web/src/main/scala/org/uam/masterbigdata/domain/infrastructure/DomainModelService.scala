package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.domain.infrastructure.model.Entities.JourneyDbo
import org.uam.masterbigdata.domain.model.Entities.Journey

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


final  class DomainModelService(dal: DataAccessLayer) extends ModelService[Future] with ComponentLogging {
  import dal.executeOperation

  override def findAllJourneys(deviceId: Long): Future[Seq[Journey]] = {
    log.info("Buscando todos los trayectos del device $deviceId")
    for {
      dbo <- dal.journeysRepository.find(deviceId)
    } yield dbo.map(dboToModel)
  }


  override def findJourneysByLabel(deviceId: Long,label: String): Future[Seq[Journey]] = {
    log.info(s"Buscando todos los trayectos por label $label del device $deviceId")
    for {
      dbo <- dal.journeysRepository.findByLabel(deviceId, label)
    } yield dbo.map(dboToModel)
  }

  override def findJourneyById(deviceId: Long, id:String): Future[Journey] = {
    log.info(s"Buscando todos los trayectos por id $id del device $deviceId")
    for {
      dbo <- dal.journeysRepository.findById(deviceId, id)
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