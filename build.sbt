lazy val `spark-challenges` =
  project
    .in(file("."))
    .enablePlugins(AutomateHeaderPlugin, JavaAppPackaging, AshScriptPlugin)
    .settings(name := "spark-challenges")
    .settings(settings)
    .settings(dockerSettings)
    .settings(
      libraryDependencies ++= dependencies,
      Docker / version := "0.1.0-SNAPSHOT"
    )

// *****************************************************************************
// Library dependencies
// *****************************************************************************

lazy val library =
  new {

    object Version {

      val sparkVersion = "3.5.4"
      val sparkTestingBase = "3.5.3_2.0.1"
      val log4j2 = "2.24.3"
      val slf4j = "2.0.16"
      val scalaCheck = "1.18.1"
      val specs2 = "4.20.9"
      val reflect = "2.13.15"
    }

    // Spark Stuff
    val sparkSQL = "org.apache.spark" %% "spark-sql" % Version.sparkVersion
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % Version.sparkTestingBase

    val slf4jAPI = "org.slf4j" % "slf4j-api" % Version.slf4j
    val slf4jBinding = "org.slf4j" % "slf4j-log4j12" % Version.slf4j
    val log4j2Api = "org.apache.logging.log4j" % "log4j-api" % Version.log4j2
    val log4j2Core = "org.apache.logging.log4j" % "log4j-core" % Version.log4j2

    val slf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % Version.slf4j

    val scalaCheck = "org.scalacheck" %% "scalacheck" % Version.scalaCheck
    val specs2 = "org.specs2" %% "specs2-core" % Version.specs2
    val specs2Mock = "org.specs2" %% "specs2-mock" % Version.specs2

    val reflect = "org.scala-lang" % "scala-reflect" % Version.reflect
  }

// *****************************************************************************
// Settings
// *****************************************************************************

lazy val settings =
  commonSettings ++
  scalafmtSettings

lazy val commonSettings =
  Seq(
    scalaVersion := "2.13.15",
    version := "0.1.0-SNAPSHOT",
    organization := "com.codingmaniacs.courses",
    headerLicense := Some(HeaderLicense.MIT("2024", "Geektimus")),
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-feature",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:postfixOps",
      "-unchecked",
      "-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-value-discard"
    ),
    Test / parallelExecution := false,
    Compile / unmanagedSourceDirectories := Seq((Compile / scalaSource).value),
    Test / unmanagedSourceDirectories := Seq((Test / scalaSource).value)
  )

lazy val dependencies =
  Seq(
    library.sparkSQL % Provided,
    library.sparkTestingBase % Test
  ) ++
  Seq(
    library.slf4jAPI % Provided,
    library.slf4jBinding % Provided,
//    library.log4j2Api,
//    library.log4j2Core
  ) ++
  Seq(
    library.scalaCheck % Test,
    library.specs2 % Test,
    library.specs2Mock % Test
  )

lazy val dockerSettings =
  Seq(
    dockerBaseImage := "openjdk:8-jdk-alpine",
    dockerUpdateLatest := true
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true
  )
