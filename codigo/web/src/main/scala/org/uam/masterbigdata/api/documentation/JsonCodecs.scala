package org.uam.masterbigdata.api.documentation

import io.circe._
//import io.circe.parser.parse
import io.circe.syntax._

import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.error.DomainError._
import org.uam.masterbigdata.domain.model.error.DomainException

import sttp.tapir.Codec.JsonCodec
import sttp.tapir.json.circe._

//import scala.collection.mutable


trait JsonCodecs {


  //Journeys
  private[api] implicit lazy val journeysViewCodec: JsonCodec[JourneyView] =
    implicitly[JsonCodec[Json]].map(json => json.as[JourneyView] match {
      case Left(_) => throw DomainException(MessageParsingError)
      case Right(value) => value
    })(coordinates => coordinates.asJson)

  private[api] implicit lazy val decodeJourneysView: Decoder[JourneyView] = (c: HCursor) => for {
    id <- c.get[String]("id")
    device_id <- c.get[String]("device_id")
    start_timestamp <- c.get[String]("start_timestamp")
    start_location_address <- c.get[String]("start_location_address")
    start_location_latitude <- c.get[String]("start_location_latitude")
    start_location_longitude <- c.get[String]("start_location_longitude")
    end_timestamp <- c.get[String]("end_timestamp")
    end_location_address <- c.get[String]("end_location_address")
    end_location_latitude <- c.get[String]("end_location_latitude")
    end_location_longitude <- c.get[String]("end_location_longitude")
    distance <- c.get[String]("distance")
  } yield JourneyView(id, device_id, start_timestamp, start_location_address, start_location_latitude, start_location_longitude, end_timestamp, end_location_address, end_location_latitude, end_location_longitude, distance)

  private[api] implicit lazy val encodeJourneysView: Encoder[JourneyView] = (a: JourneyView) => {
    Json.obj(
      ("id", a.id.asJson),
      ("device_id", a.device_id.asJson),
      ("start_timestamp", a.start_timestamp.asJson),
      ("start_location_address", a.start_location_address.asJson),
      ("start_location_latitude", a.start_location_latitude.asJson),
      ("start_location_longitude", a.start_location_longitude.asJson),
      ("end_timestamp", a.end_timestamp.asJson),
      ("end_location_address", a.end_location_address.asJson),
      ("end_location_latitude", a.end_location_latitude.asJson),
      ("end_location_longitude", a.end_location_longitude.asJson),
      ("distance", a.distance.asJson)
    )
  }

  private[api] implicit lazy val seqJourneysViewCodec: JsonCodec[Seq[JourneyView]] =
    implicitly[JsonCodec[Json]].map(json => json.as[Seq[JourneyView]](io.circe.Decoder.decodeSeq[JourneyView](decodeJourneysView)) match {
      case Left(_) =>
        throw DomainException(MessageParsingError)
      case Right(value) => value
    })(assetShape => assetShape.asJson(io.circe.Encoder.encodeSeq[JourneyView](encodeJourneysView)))

}

object JsonCodecs extends JsonCodecs
