package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.infrastructure.model.Entities.JourneyDbo
import org.uam.masterbigdata.domain.infrastructure.Profile
import com.github.tminglei.slickpg.{LTree, TsVector}
import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.domain.infrastructure.repository.JourneysRepository
import slick.ast.ColumnOption.PrimaryKey
import slick.dbio
import slick.lifted.ProvenShape
import slick.relational._
import slick.sql.SqlProfile.ColumnOption.SqlType

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global

trait RelationalRepository extends ComponentLogging  {
  self: Profile =>

  import profile.api._

  val journeyQueryTable = TableQuery[JourneyTable]
  //M A P P I N G * F U N C T I O N
  def intoJourney(row: (String, Long,Timestamp,String,Float,Float, Timestamp, String, Float,Float, Long, String)): JourneyDbo =
    JourneyDbo(row._1, row._2, row._3.getTime, row._4, row._5, row._6, row._7.getTime, row._8, row._9, row._10, row._11, row._12)

  def fromJourney(journey: JourneyDbo):Option[(String, Long,Timestamp,String,Float,Float, Timestamp, String, Float,Float, Long, String)] =
    Some((journey.id, journey.device_id, new Timestamp(journey.start_timestamp), journey.start_location_address
      , journey.start_location_latitude, journey.start_location_longitude, new Timestamp(journey.end_timestamp)
    ,journey.end_location_address, journey.end_location_latitude, journey.end_location_longitude, journey.distance, journey.label))

  /**Descripción de la tabla */
  final class JourneyTable(tag: Tag)extends Table[JourneyDbo](tag, "journeys") {
    override def * : ProvenShape[JourneyDbo] = (id, device_id, start_timestamp, start_location_address
      , start_location_latitude,start_location_longitude, end_timestamp
      , end_location_address, end_location_latitude, end_location_longitude, distance, label)<> (intoJourney, fromJourney)
    def id = column[String]("id", PrimaryKey)
    def device_id =column[ Long]("device_id")
    def start_timestamp=column[Timestamp]("start_timestamp")
    def start_location_address=column[String]("start_location_address")
    def start_location_latitude=column[Float]("start_location_latitude")
    def start_location_longitude=column[Float]("start_location_longitude")
    def end_timestamp=column[Timestamp]("end_timestamp")
    def end_location_address=column[String]("end_location_address")
    def end_location_latitude=column[Float]("end_location_latitude")
    def end_location_longitude=column[Float]("end_location_longitude")
    def distance=column[Long]("distance")
    def label=column[String]("label")
  }
  final class JourneysRelationalRepository extends JourneysRepository {
    lazy val entities = journeyQueryTable
    override def find(): dbio.DBIO[Seq[JourneyDbo]] = entities.result

    override def find(name: String): dbio.DBIO[Seq[JourneyDbo]] = {
      val search = entities.filter(_.label === name)
      search.result
    }

    override def find(id: Long): dbio.DBIO[JourneyDbo] = ???
  }
}
