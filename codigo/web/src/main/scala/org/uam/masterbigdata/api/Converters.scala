package org.uam.masterbigdata.api

import org.uam.masterbigdata.api.ApiModel.BuildInfoDto
import org.uam.masterbigdata.api.model.BuildInfo
import org.uam.masterbigdata.domain.AdapterModel.{EventView, JourneyView, FrameView}
import org.uam.masterbigdata.domain.model.Entities.{Event, Journey, Frame}

import scala.concurrent.Future

trait Converters {
  import Converters._

  def buildInfoToApi(): BuildInfoDto = {
    BuildInfoDto(
      BuildInfo.name,
      BuildInfo.version
    )
  }

  def modelToApi(model:Journey): JourneyView = {
    JourneyView(
      model.id
      ,model.device_id
      ,model.start_timestamp
      ,model.start_location_address
      ,model.start_location_latitude
      ,model.start_location_longitude
      ,model.end_timestamp
      ,model.end_location_address
      ,model.end_location_latitude
      ,model.end_location_longitude
      ,model.distance
    )
  }

  def modelToApi(model: Event): EventView = {
    EventView(
      model.id
      , model.device_id
      , model.created
      , model.type_id
      , model.location_address
      , model.location_latitude
      , model.location_longitude
      , model.value
    )
  }

  /*
  def modelToApi(model: Frame): FrameView = {
    EventView(
      model.id
      , model.device_id
      , model.created
      , model
      , model.location_address
      , model.location_latitude
      , model.location_longitude
      , model.value
    )
  }    */

  def asRightFuture[T](t: T): Future[Either[Nothing, T]] =
    Future.successful(Right(t))
}

object Converters extends Converters {

}
