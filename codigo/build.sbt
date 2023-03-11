//Modificada para que cuadre con la aplicación web pero es una versión mayor a la que teniamos para la prueba de spark
//Ver si funciona o hay que modificar
//con las dependecias del sub projecto de spark podemos hacer lo mismo que con las web
ThisBuild / scalaVersion := "2.12.12"

lazy val batch = (project in file("batch_layer"))
  .settings(name := "Batch_Layer")
  .settings(libraryDependencies ++= BatchDependencies.production)
  .settings(libraryDependencies ++= BatchDependencies.test)


lazy val web = (project in file("web"))
  .settings(name := "web")
  .settings(libraryDependencies ++= WebDependencies.production)


//Para trastear
lazy val sandbox = (project in file("sandbox"))
  .settings(name := "sandbox")
  .settings(libraryDependencies ++= SandboxDependencies.production)
  .settings(libraryDependencies ++= SandboxDependencies.test)



