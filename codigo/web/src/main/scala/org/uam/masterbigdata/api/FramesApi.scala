package org.uam.masterbigdata.api

import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.api.documentation.ApiMapper
import org.uam.masterbigdata.domain.service.FramesService

import scala.concurrent.ExecutionContext.Implicits.global

class FramesApi (service:FramesService) extends ApiMapper with ComponentLogging {

}

object FramesApi{
  def apply(service:FramesService) = new FramesApi(service)
}

