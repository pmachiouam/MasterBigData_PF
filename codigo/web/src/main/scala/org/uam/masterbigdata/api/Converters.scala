package org.uam.masterbigdata.api

import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.uam.masterbigdata.api.ApiModel.BuildInfoDto
import org.uam.masterbigdata.api.model.BuildInfo
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

  def asRightFuture[T](t: T): Future[Either[Nothing, T]] =
    Future.successful(Right(t))
}

object Converters extends Converters {

}
