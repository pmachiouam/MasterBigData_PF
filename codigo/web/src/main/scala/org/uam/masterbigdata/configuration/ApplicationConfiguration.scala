package org.uam.masterbigdata.configuration
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import akka.stream.Materializer
import com.typesafe.config.{Config, ConfigFactory}
import org.uam.masterbigdata.api.{ActuatorApi, SwaggerApi}

trait ApplicationConfiguration {
  //AKKA
  val system: ActorSystem
  implicit val materializer: Materializer

  val config: Config = ConfigFactory.load()

  //Propio servidor web
  private val serverPath = "application.server"

  val serverAddress: String = config.getString(s"$serverPath.interface")
  val serverPort: Int = config.getInt(s"$serverPath.port")

  //R U T A S
  lazy val routes: Route =
    ActuatorApi.route ~ SwaggerApi.route

  // P O S T G R E S Q L   D A T A B A S E

}
