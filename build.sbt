ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.14"
ThisBuild / scalaBinaryVersion := "2.12"

lazy val root = (project in file("."))
  .settings(
    name := "1"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "com.opencagedata" %% "scala-opencage-geocoder" % "1.1.1",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "ch.hsr" % "geohash" % "1.4.0"
)
