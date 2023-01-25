ThisBuild / scalaVersion := "2.12.10"
val sparkVersion = "3.2.3"
val vegasVersion = "0.3.11"
val postgresVersion = "42.5.1"
//val log4jVersion = "2.19.0"
val scalaTestVersion = "3.2.15"

lazy val batch = (project in file("./batch_layer"))
  .settings(
    name := "Batch_Layer",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      // logging
      // "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      // "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
      // postgres for DB connectivity
      "org.postgresql" % "postgresql" % postgresVersion,
      // Test
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val web = (project in file("./web"))
  .settings(name := "web")
  .settings(libraryDependencies ++= WebDependencies.production)
  .settings(libraryDependencies ++= Seq(
    "org.postgresql" % "postgresql" % postgresVersion,
    // Test
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test)
  )

