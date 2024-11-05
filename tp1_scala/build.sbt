ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "tp1_scala",
    idePackagePrefix := Some("fr.umontpelleir.ig5")
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
libraryDependencies += "org.neo4j" %% "neo4j-connector-apache-spark" % "5.3.1_for_spark_3"