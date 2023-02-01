package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.domain.infrastructure.model.Entities.{JourneyDbo, EventDbo, FrameDbo}
import org.uam.masterbigdata.domain.model.Entities.{Journey, Event, Frame}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


final  class DomainModelService(dal: DataAccessLayer) extends ModelService[Future] with ComponentLogging {
  import dal.executeOperation
//Journeys
  override def findAllJourneys(deviceId: Long): Future[Seq[Journey]] = {
    log.info("Buscando todos los trayectos del device $deviceId")
    for {
      dbo <- dal.journeysRepository.find(deviceId)
    } yield dbo.map(dboToModel)
  }

  private def dboToModel(dbo: JourneyDbo): Journey =
    Journey(dbo.id, dbo.device_id.toString, dbo.start_timestamp.toString, dbo.start_location_address, dbo.start_location_latitude.toString
      , dbo.start_location_longitude.toString, dbo.end_timestamp.toString, dbo.end_location_address, dbo.end_location_latitude.toString
      , dbo.end_location_longitude.toString, dbo.distance.toString)

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



  //Events
  private def dboToModel(dbo: EventDbo): Event =
    Event(dbo.id.toString, dbo.device_id.toString, dbo.created.toString, dbo.type_id.toString, dbo.location_address
      , dbo.location_latitude.toString, dbo.location_longitude.toString, dbo.value)
  override def findAllEvents(deviceId: Long): Future[Seq[Event]] =
      for{
          dbo <- dal.eventsRepository.find(deviceId)
      }yield dbo.map(dboToModel)

  override def findEventById(deviceId: Long, id: Long): Future[Event] =
    for{
      dbo <- dal.eventsRepository.findById(deviceId, id)
    }yield dboToModel(dbo)

  //Frames
  override def findAllFrames(deviceId: Long): Future[Seq[Frame]] = ???

  override def findFrameById(deviceId: Long, id: Long): Future[Frame] = ???
}

object DomainModelService {
  def apply(dal: DataAccessLayer) = new DomainModelService(dal)
}