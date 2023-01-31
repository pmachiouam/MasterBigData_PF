package org.uam.masterbigdata.domain.model

object Entities {
  case class Journey(
                          id: String
                          , device_id: String
                          , start_timestamp: String
                          , start_location_address: String
                          , start_location_latitude: String
                          , start_location_longitude: String
                          , end_timestamp: String
                          , end_location_address: String
                          , end_location_latitude: String
                          , end_location_longitude: String
                          , distance: String
                        )
  case class JourneysByDeviceIdRequest(deviceId:Long)
  case class JourneysByDeviceIdAndLabelRequest(deviceId:Long, label:String)
  case class JourneyByDeviceIdRequest(deviceId:Long, journeyId:String)
}
