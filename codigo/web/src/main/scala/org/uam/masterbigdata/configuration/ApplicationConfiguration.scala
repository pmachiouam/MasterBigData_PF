package org.uam.masterbigdata.configuration
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import akka.stream.Materializer
import com.typesafe.config.{Config, ConfigFactory}
import org.uam.masterbigdata.api.{ActuatorApi, SwaggerApi, JourneysApi}
import org.uam.masterbigdata.domain.infrastructure.JourneyServiceBase
import org.uam.masterbigdata.domain.service.JourneysService

trait ApplicationConfiguration {
  //AKKA
  val system: ActorSystem
  implicit val materializer: Materializer

  private val config: Config = ConfigFactory.load()

  //Propio servidor web
  private val serverPath = "application.server"

  val serverAddress: String = config.getString(s"$serverPath.interface")
  val serverPort: Int = config.getInt(s"$serverPath.port")

  //Journeys
  private val journeysService:JourneysService = JourneyServiceBase()
  private val journeysApi: JourneysApi = JourneysApi(journeysService)

  //R U T A S
  lazy val routes: Route =
    ActuatorApi.route ~ SwaggerApi.route ~ journeysApi.routes

  // P O S T G R E S Q L   D A T A B A S E

}
