import Dependencies.{Version => CommonVersion}
import sbt._

object KafkaProducerDependencies {
  val production: List[ModuleID] =
  // T A P I R
    "com.softwaremill.sttp.tapir" %% "tapir-core" % CommonVersion.tapir ::
      "com.softwaremill.sttp.tapir" %% "tapir-akka-http-server" % CommonVersion.tapir ::
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % CommonVersion.tapir ::
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % CommonVersion.tapir ::
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % CommonVersion.tapir ::
      "com.softwaremill.sttp.tapir" %% "tapir-sttp-client" % CommonVersion.tapir ::
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-akka-http" % CommonVersion.tapir ::
      // J S O N  L I B S
      "io.circe" %% "circe-optics" % CommonVersion.circe ::
      "io.circe" %% "circe-generic-extras" % CommonVersion.circe ::
      "io.circe" %% "circe-shapes" % CommonVersion.circe ::
      // A K K A
      "com.typesafe.akka" %% "akka-http" % CommonVersion.akkaHttp ::
      "com.typesafe.akka" %% "akka-http-spray-json" % CommonVersion.akkaHttp ::
      "com.typesafe.akka" %% "akka-slf4j" % CommonVersion.akka ::
      "com.typesafe.akka" %% "akka-stream" % CommonVersion.akka ::
      "com.typesafe.akka" %% "akka-remote" % CommonVersion.akka ::
      // kafka
      "org.apache.kafka" %% "kafka" % Version.kafkaVersion ::
      "org.apache.kafka" % "kafka-streams" % Version.kafkaVersion :: Nil


  val test: List[ModuleID] = "org.scalatest" %% "scalatest" % CommonVersion.scalaTest % "test" :: Nil

  object Version {
    val kafkaVersion = "2.4.0"
  }
}
