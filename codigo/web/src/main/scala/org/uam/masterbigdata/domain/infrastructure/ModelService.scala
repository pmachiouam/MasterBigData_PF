package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.model.Entities.Journey

import scala.concurrent.Future
/**
 * Traits de los services
 * * @tparam F type constructor
 * */
trait ModelService[F[_]] {
  def findAllJourneys(deviceId:Long):Future[Seq[Journey]]
  def findJourneysByLabel(deviceId:Long,label:String):Future[Seq[Journey]]
  def findJourneyById(deviceId:Long,id:String):Future[Journey]
}
