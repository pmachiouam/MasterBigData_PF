package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.model.Entities.Journey

import scala.concurrent.Future
/**
 * Traits de los services
 * * @tparam F type constructor
 * */
trait ModelService[F[_]] {
  def findAllJourneys():Future[Seq[Journey]]
  def findJourneysByLabel(label:String):Future[Seq[Journey]]
  def findJourneyById(id:Long):Future[Journey]
}
