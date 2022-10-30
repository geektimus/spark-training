lazy val `spark-challenges` =
  project
    .in(file("."))
    .enablePlugins(AutomateHeaderPlugin, GitVersioning, JavaAppPackaging, AshScriptPlugin)
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

      val sparkVersion = "3.3.0"
      val sparkTestingBase = "3.3.0_1.2.0"
      val log4j2 = "2.19.0"
      val slf4j = "2.0.3"
      val scalaCheck = "1.17.0"
      val specs2 = "4.17.0"
      val reflect = "2.13.10"
    }

    // Spark Stuff
    val sparkSQL = "org.apache.spark" %% "spark-sql" % Version.sparkVersion
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % Version.sparkTestingBase

    val slf4jAPI = "org.slf4j" % "slf4j-api" % Version.slf4j
    val slf4jNOP = "org.slf4j" % "slf4j-nop" % Version.slf4j
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
  gitSettings ++
  scalafmtSettings

lazy val commonSettings =
  Seq(
    scalaVersion := "2.12.12",
    version := "0.1.0-SNAPSHOT",
    organization := "com.codingmaniacs.courses",
    headerLicense := Some(HeaderLicense.MIT("2020", "Geektimus")),
    scalacOptions ++= Seq(
        "-deprecation",
        "-encoding",
        "UTF-8",
        "-feature",
        "-language:existentials",
        "-language:higherKinds",
        "-language:implicitConversions",
        "-language:postfixOps",
        "-target:jvm-1.8",
        "-unchecked",
        "-Xfatal-warnings",
        "-Xlint",
        "-Yno-adapted-args",
        "-Ypartial-unification",
        "-Ywarn-dead-code",
        "-Ywarn-infer-any",
        "-Ywarn-numeric-widen",
        "-Ywarn-unused",
        "-Ywarn-unused-import",
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
    library.slf4jNOP % Test,
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

lazy val gitSettings =
  Seq(
    git.useGitDescribe := true
  )

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true
  )
