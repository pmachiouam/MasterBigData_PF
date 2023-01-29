package org.uam.masterbigdata.domain.infrastructure.repository

import org.uam.masterbigdata.domain.infrastructure.model.Entities.JourneyDbo
import slick.dbio.DBIO
trait JourneysRepository {
  def find():DBIO[Seq[JourneyDbo]]
  def find(name:String):DBIO[Seq[JourneyDbo]]
  def find(id:Long):DBIO[JourneyDbo]
}
