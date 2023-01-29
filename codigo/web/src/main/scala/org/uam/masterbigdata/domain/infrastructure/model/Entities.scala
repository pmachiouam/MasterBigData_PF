package org.uam.masterbigdata.domain.infrastructure.model

object Entities {
  case class JourneyDbo(
                      id: String
                      , device_id: Long
                      , start_timestamp: Long
                      , start_location_address: String
                      , start_location_latitude: Float
                      , start_location_longitude: Float
                      , end_timestamp: Long
                      , end_location_address: String
                      , end_location_latitude: Float
                      , end_location_longitude: Float
                      , distance: Long
                      , label:String
                    )


}
