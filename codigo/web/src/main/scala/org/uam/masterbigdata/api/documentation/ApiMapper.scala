package org.uam.masterbigdata.api.documentation

import org.uam.masterbigdata.domain.model.Entities.{JourneyByDeviceIdRequest, JourneysByDeviceIdAndLabelRequest, JourneysByDeviceIdRequest}

trait ApiMapper {
  private[api] lazy val mapToDeviceJourneyRequest: ((Long, String)) => JourneyByDeviceIdRequest =
    request => JourneyByDeviceIdRequest(request._1, request._2)

  private[api] lazy val mapToDeviceJourneysByLabelRequest:  ((Long, String))  => JourneysByDeviceIdAndLabelRequest =
    request => JourneysByDeviceIdAndLabelRequest( request._1, request._2)


  private[api] lazy val mapToDeviceJourneysRequest: Long => JourneysByDeviceIdRequest =
    request => JourneysByDeviceIdRequest(request)

}
