ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-docker"
  )

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.2" % "provided",
  "org.apache.spark" %% "spark-core" % "3.5.2",
  "com.mysql" % "mysql-connector-j" % "8.0.33")


