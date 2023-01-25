import Dependencies.{Version => CommonVersion}
import sbt._

object WebDependencies {
  val production: Seq[ModuleID] = Seq(
    // J S O N  L I B S
    "io.circe" %% "circe-optics" % CommonVersion.circe,
    "org.json4s" %% "json4s-native" % CommonVersion.json4s,

    // T A P I R
    "com.softwaremill.sttp.tapir" %% "tapir-core" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-akka-http-server" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-sttp-client" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-akka-http" % CommonVersion.tapir,

    // A K K A
  "com.typesafe.akka" %% "akka-http" % CommonVersion.akkaHttp,
  "com.typesafe.akka" %% "akka-http-spray-json" % CommonVersion.akkaHttp,
  "com.typesafe.akka" %% "akka-slf4j" % CommonVersion.akka,
  "com.typesafe.akka" %% "akka-stream" % CommonVersion.akka,
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % CommonVersion.akkaSlick
  exclude("com.typesafe", "config")
  exclude("com.typesafe.akka", "akka-actor")
  )
}
