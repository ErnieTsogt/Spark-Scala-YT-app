ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "Spark-Scala-YT-app"
  )

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.2" ,
  "org.apache.spark" %% "spark-core" % "3.5.2",
  "com.mysql" % "mysql-connector-j" % "8.0.33",
  "org.scalatest" %% "scalatest" % "3.2.19" ,
  "org.scalatest" %% "scalatest" % "3.2.19" % "Test")

fork := true

javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)


