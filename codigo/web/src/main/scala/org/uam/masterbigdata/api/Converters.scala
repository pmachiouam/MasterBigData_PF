package org.uam.masterbigdata.api

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.uam.masterbigdata.api.ApiModel.BuildInfoDto
import org.uam.masterbigdata.api.model.BuildInfo
import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.Entities.Journey
import sttp.tapir.{Schema, Validator}

import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.concurrent.Future
import scala.util.Success

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

  def asRightFuture[T](t: T): Future[Either[Nothing, T]] =
    Future.successful(Right(t))
}

object Converters extends Converters {

}
